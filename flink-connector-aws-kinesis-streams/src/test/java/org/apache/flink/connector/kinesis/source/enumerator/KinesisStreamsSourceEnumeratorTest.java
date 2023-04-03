/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kinesis.source.enumerator;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.connector.kinesis.source.config.SourceConfigConstants;
import org.apache.flink.connector.kinesis.source.config.SourceConfigConstants.InitialPosition;
import org.apache.flink.connector.kinesis.source.model.CompletedShardsEvent;
import org.apache.flink.connector.kinesis.source.proxy.StreamProxy;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;
import org.apache.flink.connector.kinesis.source.util.TestUtil;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableSet;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.flink.connector.kinesis.source.util.KinesisStreamProxyProvider.TestKinesisStreamProxy;
import static org.apache.flink.connector.kinesis.source.util.KinesisStreamProxyProvider.getTestStreamProxy;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.generateShardId;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.getTestSplit;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatNoException;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

class KinesisStreamsSourceEnumeratorTest {

    private static final int NUM_SUBTASKS = 5;
    private static final String STREAM_ARN =
            "arn:aws:kinesis:us-east-1:123456789012:stream/keenesesStream";

    @ParameterizedTest
    @MethodSource("provideInitialPositions")
    void testStartWithoutStateDiscoversAndAssignsShards(
            InitialPosition initialPosition,
            String initialTimestamp,
            ShardIteratorType expectedShardIteratorType)
            throws Throwable {
        try (MockSplitEnumeratorContext<KinesisShardSplit> context =
                new MockSplitEnumeratorContext<>(NUM_SUBTASKS)) {
            TestKinesisStreamProxy streamProxy = getTestStreamProxy();
            final Properties consumerConfig = new Properties();
            consumerConfig.setProperty(
                    SourceConfigConstants.STREAM_INITIAL_POSITION, initialPosition.name());
            consumerConfig.setProperty(
                    SourceConfigConstants.STREAM_INITIAL_TIMESTAMP, initialTimestamp);

            // Given enumerator is initialized with no state
            KinesisStreamsSourceEnumerator enumerator =
                    new KinesisStreamsSourceEnumerator(
                            context, STREAM_ARN, consumerConfig, streamProxy, null);
            // When enumerator starts
            enumerator.start();
            // Then initial discovery scheduled, with periodic discovery after
            assertThat(context.getOneTimeCallables()).hasSize(1);
            assertThat(context.getPeriodicCallables()).hasSize(1);

            // Given there is one registered reader, with 4 shards in stream
            final int subtaskId = 1;
            context.registerReader(TestUtil.getTestReaderInfo(subtaskId));
            enumerator.addReader(subtaskId);
            String[] shardIds =
                    new String[] {
                        generateShardId(0),
                        generateShardId(1),
                        generateShardId(2),
                        generateShardId(3)
                    };
            streamProxy.addShards(shardIds);
            // When first discovery runs
            context.runNextOneTimeCallable();
            SplitsAssignment<KinesisShardSplit> initialSplitAssignment =
                    context.getSplitsAssignmentSequence().get(0);
            // Then all 4 shards discovered on startup with configured INITIAL_POSITION
            assertThat(initialSplitAssignment.assignment()).containsOnlyKeys(subtaskId);
            assertThat(
                            initialSplitAssignment.assignment().get(subtaskId).stream()
                                    .map(KinesisShardSplit::getShardId))
                    .containsExactly(shardIds);
            assertThat(
                            initialSplitAssignment.assignment().get(subtaskId).stream()
                                    .map(KinesisShardSplit::getStartingPosition))
                    .allSatisfy(
                            s ->
                                    assertThat(s.getShardIteratorType())
                                            .isEqualTo(expectedShardIteratorType));

            // Given no resharding occurs (list of shards remains the same)
            // When first periodic discovery runs
            context.runPeriodicCallable(0);
            // Then no additional splits are assigned
            SplitsAssignment<KinesisShardSplit> noUpdateSplitAssignment =
                    context.getSplitsAssignmentSequence().get(1);
            assertThat(noUpdateSplitAssignment.assignment()).isEmpty();

            // Given resharding occurs
            String[] additionalShards = new String[] {generateShardId(4), generateShardId(5)};
            streamProxy.addShards(additionalShards);
            // When periodic discovery runs
            context.runPeriodicCallable(0);
            // Then only additional shards are assigned to read from TRIM_HORIZON
            SplitsAssignment<KinesisShardSplit> afterReshardingSplitAssignment =
                    context.getSplitsAssignmentSequence().get(2);
            assertThat(afterReshardingSplitAssignment.assignment()).containsOnlyKeys(subtaskId);
            assertThat(
                            afterReshardingSplitAssignment.assignment().get(subtaskId).stream()
                                    .map(KinesisShardSplit::getShardId))
                    .containsExactly(additionalShards);
            assertThat(
                            afterReshardingSplitAssignment.assignment().get(subtaskId).stream()
                                    .map(KinesisShardSplit::getStartingPosition))
                    .allSatisfy(
                            s ->
                                    assertThat(s.getShardIteratorType())
                                            .isEqualTo(ShardIteratorType.TRIM_HORIZON));
        }
    }

    @ParameterizedTest
    @MethodSource("provideInitialPositions")
    void testStartWithStateDoesNotAssignCompletedShards(
            InitialPosition initialPosition,
            String initialTimestamp,
            ShardIteratorType expectedShardIteratorType)
            throws Throwable {
        try (MockSplitEnumeratorContext<KinesisShardSplit> context =
                new MockSplitEnumeratorContext<>(NUM_SUBTASKS)) {
            TestKinesisStreamProxy streamProxy = getTestStreamProxy();
            final String completedShard = generateShardId(0);
            final String lastSeenShard = generateShardId(1);
            final Set<String> completedShardIds = ImmutableSet.of(completedShard);

            KinesisStreamsSourceEnumeratorState state =
                    new KinesisStreamsSourceEnumeratorState(
                            completedShardIds, Collections.emptySet(), lastSeenShard);

            final Properties consumerConfig = new Properties();
            consumerConfig.setProperty(
                    SourceConfigConstants.STREAM_INITIAL_POSITION, initialPosition.name());
            consumerConfig.setProperty(
                    SourceConfigConstants.STREAM_INITIAL_TIMESTAMP, initialTimestamp);

            // Given enumerator is initialised with state
            KinesisStreamsSourceEnumerator enumerator =
                    new KinesisStreamsSourceEnumerator(
                            context, STREAM_ARN, consumerConfig, streamProxy, state);
            // When enumerator starts
            enumerator.start();
            // Then no initial discovery is scheduled, but a periodic discovery is scheduled
            assertThat(context.getOneTimeCallables()).isEmpty();
            assertThat(context.getPeriodicCallables()).hasSize(1);

            // Given there is one registered reader, with 4 shards in stream
            final int subtaskId = 1;
            context.registerReader(TestUtil.getTestReaderInfo(subtaskId));
            enumerator.addReader(subtaskId);
            String[] shardIds =
                    new String[] {
                        completedShard, lastSeenShard, generateShardId(2), generateShardId(3)
                    };
            streamProxy.addShards(shardIds);
            // When first periodic discovery of shards
            context.runPeriodicCallable(0);
            // Then newer shards will be discovered and read from TRIM_HORIZON, independent of
            // configured starting position
            SplitsAssignment<KinesisShardSplit> firstUpdateSplitAssignment =
                    context.getSplitsAssignmentSequence().get(0);
            assertThat(firstUpdateSplitAssignment.assignment()).containsOnlyKeys(subtaskId);
            assertThat(
                            firstUpdateSplitAssignment.assignment().get(subtaskId).stream()
                                    .map(KinesisShardSplit::getShardId))
                    .containsExactly(generateShardId(2), generateShardId(3));
            assertThat(
                            firstUpdateSplitAssignment.assignment().get(subtaskId).stream()
                                    .map(KinesisShardSplit::getStartingPosition))
                    .allSatisfy(
                            s ->
                                    assertThat(s.getShardIteratorType())
                                            .isEqualTo(ShardIteratorType.TRIM_HORIZON));
        }
    }

    @Test
    void testReturnedSplitsWillBeReassigned() throws Throwable {
        try (MockSplitEnumeratorContext<KinesisShardSplit> context =
                new MockSplitEnumeratorContext<>(NUM_SUBTASKS)) {
            TestKinesisStreamProxy streamProxy = getTestStreamProxy();
            KinesisStreamsSourceEnumerator enumerator =
                    getSimpleEnumeratorWithNoState(context, streamProxy);

            // Given enumerator is initialised with one registered reader, with 4 shards in stream
            final int subtaskId = 1;
            context.registerReader(TestUtil.getTestReaderInfo(subtaskId));
            enumerator.addReader(subtaskId);
            String[] shardIds =
                    new String[] {
                        generateShardId(0),
                        generateShardId(1),
                        generateShardId(2),
                        generateShardId(3)
                    };
            streamProxy.addShards(shardIds);

            // When first discovery runs
            context.runNextOneTimeCallable();
            SplitsAssignment<KinesisShardSplit> initialSplitAssignment =
                    context.getSplitsAssignmentSequence().get(0);

            // Then all 4 shards discovered on startup
            assertThat(initialSplitAssignment.assignment()).containsOnlyKeys(subtaskId);
            assertThat(
                            initialSplitAssignment.assignment().get(subtaskId).stream()
                                    .map(KinesisShardSplit::getShardId))
                    .containsExactly(shardIds);

            // Given one shard split is returned
            KinesisShardSplit returnedSplit =
                    initialSplitAssignment.assignment().get(subtaskId).get(0);
            enumerator.addSplitsBack(ImmutableList.of(returnedSplit), subtaskId);

            // When first periodic discovery runs
            context.runPeriodicCallable(0);
            // Then returned split will be assigned
            SplitsAssignment<KinesisShardSplit> firstReturnedSplitAssignment =
                    context.getSplitsAssignmentSequence().get(1);
            assertThat(firstReturnedSplitAssignment.assignment()).containsOnlyKeys(subtaskId);
            assertThat(firstReturnedSplitAssignment.assignment().get(subtaskId))
                    .containsExactly(returnedSplit);
        }
    }

    @Test
    void testAddSplitsBackWithoutSplitIsNoOp() throws Throwable {
        try (MockSplitEnumeratorContext<KinesisShardSplit> context =
                new MockSplitEnumeratorContext<>(NUM_SUBTASKS)) {
            TestKinesisStreamProxy streamProxy = getTestStreamProxy();
            KinesisStreamsSourceEnumerator enumerator =
                    getSimpleEnumeratorWithNoState(context, streamProxy);
            List<KinesisShardSplit> splits = ImmutableList.of(getTestSplit());

            // Given enumerator has no assigned splits
            // When we add splits back
            // Then handled gracefully with no exception thrown
            assertThatNoException().isThrownBy(() -> enumerator.addSplitsBack(splits, 1));
        }
    }

    @Test
    void testHandleSplitRequestIsNoOp() throws Throwable {
        try (MockSplitEnumeratorContext<KinesisShardSplit> context =
                new MockSplitEnumeratorContext<>(NUM_SUBTASKS)) {
            TestKinesisStreamProxy streamProxy = getTestStreamProxy();
            KinesisStreamsSourceEnumerator enumerator =
                    getSimpleEnumeratorWithNoState(context, streamProxy);

            // Given enumerator has no assigned splits
            // When we add splits back
            // Then handled gracefully with no exception thrown
            assertThatNoException()
                    .isThrownBy(() -> enumerator.handleSplitRequest(1, "some-hostname"));
        }
    }

    @Test
    void testAssignSplitsSurfacesThrowableIfUnableToListShards() throws Throwable {
        try (MockSplitEnumeratorContext<KinesisShardSplit> context =
                new MockSplitEnumeratorContext<>(NUM_SUBTASKS)) {
            TestKinesisStreamProxy streamProxy = getTestStreamProxy();
            final Properties consumerConfig = new Properties();
            KinesisStreamsSourceEnumerator enumerator =
                    new KinesisStreamsSourceEnumerator(
                            context, STREAM_ARN, consumerConfig, streamProxy, null);
            enumerator.start();

            // Given List Shard request throws an Exception
            streamProxy.setListShardsExceptionSupplier(
                    () -> AwsServiceException.create("Internal Service Error", null));

            // When first discovery runs
            // Then runtime exception is thrown
            assertThatExceptionOfType(FlinkRuntimeException.class)
                    .isThrownBy(context::runNextOneTimeCallable)
                    .withMessage("Failed to list shards.")
                    .withStackTraceContaining("Internal Service Error");
        }
    }

    @Test
    void testAssignSplitsHandlesRepeatSplitsGracefully() throws Throwable {
        try (MockSplitEnumeratorContext<KinesisShardSplit> context =
                new MockSplitEnumeratorContext<>(NUM_SUBTASKS)) {
            TestKinesisStreamProxy streamProxy = getTestStreamProxy();
            final Properties consumerConfig = new Properties();
            KinesisStreamsSourceEnumerator enumerator =
                    new KinesisStreamsSourceEnumerator(
                            context, STREAM_ARN, consumerConfig, streamProxy, null);
            enumerator.start();

            // Given enumerator is initialised with one registered reader, with 4 shards in stream
            final int subtaskId = 1;
            context.registerReader(TestUtil.getTestReaderInfo(subtaskId));
            enumerator.addReader(subtaskId);
            String[] shardIds =
                    new String[] {
                        generateShardId(0),
                        generateShardId(1),
                        generateShardId(2),
                        generateShardId(3)
                    };
            streamProxy.addShards(shardIds);

            // When first discovery runs
            context.runNextOneTimeCallable();
            SplitsAssignment<KinesisShardSplit> initialSplitAssignment =
                    context.getSplitsAssignmentSequence().get(0);

            // Then all 4 shards discovered on startup
            assertThat(initialSplitAssignment.assignment()).containsOnlyKeys(subtaskId);
            assertThat(
                            initialSplitAssignment.assignment().get(subtaskId).stream()
                                    .map(KinesisShardSplit::getShardId))
                    .containsExactly(shardIds);

            // Given ListShards doesn't respect lastSeenShardId, and returns already assigned shards
            streamProxy.setShouldRespectLastSeenShardId(false);

            // When first periodic discovery runs
            // Then handled gracefully
            assertThatNoException().isThrownBy(() -> context.runPeriodicCallable(0));
            SplitsAssignment<KinesisShardSplit> secondReturnedSplitAssignment =
                    context.getSplitsAssignmentSequence().get(1);
            assertThat(secondReturnedSplitAssignment.assignment()).isEmpty();
        }
    }

    @Test
    void testAssignSplitIgnoresCompletedShards() throws Throwable {
        try (MockSplitEnumeratorContext<KinesisShardSplit> context =
                new MockSplitEnumeratorContext<>(NUM_SUBTASKS)) {
            TestKinesisStreamProxy streamProxy = getTestStreamProxy();
            final Properties consumerConfig = new Properties();
            KinesisStreamsSourceEnumerator enumerator =
                    new KinesisStreamsSourceEnumerator(
                            context, STREAM_ARN, consumerConfig, streamProxy, null);
            enumerator.start();

            // Given enumerator is initialised with one registered reader, with 4 shards in stream
            final int subtaskId = 1;
            context.registerReader(TestUtil.getTestReaderInfo(subtaskId));
            enumerator.addReader(subtaskId);
            String[] shardIds =
                    new String[] {
                        generateShardId(0),
                        generateShardId(1),
                        generateShardId(2),
                        generateShardId(3)
                    };
            streamProxy.addShards(shardIds);
            // Given also that one shard is marked as complete
            enumerator.handleSourceEvent(
                    subtaskId,
                    new CompletedShardsEvent(
                            ImmutableSet.of(getTestSplit(STREAM_ARN, shardIds[0]).splitId())));

            // When first discovery runs
            context.runNextOneTimeCallable();
            SplitsAssignment<KinesisShardSplit> initialSplitAssignment =
                    context.getSplitsAssignmentSequence().get(0);

            // Then all 4 shards discovered on startup
            assertThat(initialSplitAssignment.assignment()).containsOnlyKeys(subtaskId);
            assertThat(
                            initialSplitAssignment.assignment().get(subtaskId).stream()
                                    .map(KinesisShardSplit::getShardId))
                    .containsExactly(Arrays.copyOfRange(shardIds, 1, shardIds.length));
        }
    }

    @Test
    void testAssignSplitWithoutRegisteredReaders() throws Throwable {
        try (MockSplitEnumeratorContext<KinesisShardSplit> context =
                new MockSplitEnumeratorContext<>(NUM_SUBTASKS)) {
            TestKinesisStreamProxy streamProxy = getTestStreamProxy();
            final Properties consumerConfig = new Properties();
            KinesisStreamsSourceEnumerator enumerator =
                    new KinesisStreamsSourceEnumerator(
                            context, STREAM_ARN, consumerConfig, streamProxy, null);
            enumerator.start();

            // Given enumerator is initialised without a reader
            String[] shardIds =
                    new String[] {
                        generateShardId(0),
                        generateShardId(1),
                        generateShardId(2),
                        generateShardId(3)
                    };
            streamProxy.addShards(shardIds);

            // When first discovery runs
            context.runNextOneTimeCallable();

            // Then nothing is assigned
            assertThat(context.getSplitsAssignmentSequence()).isEmpty();

            // Given a reader is now registered
            final int subtaskId = 1;
            context.registerReader(TestUtil.getTestReaderInfo(subtaskId));
            enumerator.addReader(subtaskId);

            // When next periodic discovery is run
            context.runPeriodicCallable(0);
            SplitsAssignment<KinesisShardSplit> initialSplitAssignment =
                    context.getSplitsAssignmentSequence().get(0);

            // Then shards are assigned, still respecting original configuration
            assertThat(initialSplitAssignment.assignment()).containsOnlyKeys(subtaskId);
            assertThat(
                            initialSplitAssignment.assignment().get(subtaskId).stream()
                                    .map(KinesisShardSplit::getShardId))
                    .containsExactlyInAnyOrder(shardIds);
            assertThat(
                            initialSplitAssignment.assignment().get(subtaskId).stream()
                                    .map(KinesisShardSplit::getStartingPosition))
                    .allSatisfy(
                            s ->
                                    assertThat(s.getShardIteratorType())
                                            .isEqualTo(ShardIteratorType.AT_TIMESTAMP));
        }
    }

    @Test
    void testRestoreFromStateRemembersLastSeenShardId() throws Throwable {
        try (MockSplitEnumeratorContext<KinesisShardSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                MockSplitEnumeratorContext<KinesisShardSplit> restoredContext =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS)) {
            TestKinesisStreamProxy streamProxy = getTestStreamProxy();
            final Properties consumerConfig = new Properties();
            KinesisStreamsSourceEnumerator enumerator =
                    new KinesisStreamsSourceEnumerator(
                            context, STREAM_ARN, consumerConfig, streamProxy, null);
            enumerator.start();

            // Given enumerator is initialised with one registered reader, with 4 shards in stream
            final int subtaskId = 1;
            context.registerReader(TestUtil.getTestReaderInfo(subtaskId));
            enumerator.addReader(subtaskId);
            String[] shardIds =
                    new String[] {
                        generateShardId(0),
                        generateShardId(1),
                        generateShardId(2),
                        generateShardId(3)
                    };
            streamProxy.addShards(shardIds);
            // Given enumerator has run discovery
            context.runNextOneTimeCallable();

            // When restored from state
            KinesisStreamsSourceEnumeratorState snapshottedState = enumerator.snapshotState(1);
            KinesisStreamsSourceEnumerator restoredEnumerator =
                    new KinesisStreamsSourceEnumerator(
                            restoredContext,
                            STREAM_ARN,
                            consumerConfig,
                            streamProxy,
                            snapshottedState);
            restoredEnumerator.start();
            // Given enumerator is initialised with one registered reader, with 4 shards in stream
            restoredContext.registerReader(TestUtil.getTestReaderInfo(subtaskId));
            restoredEnumerator.addReader(subtaskId);
            restoredContext.runPeriodicCallable(0);

            // Then ListShards receives a ListShards call with the lastSeenShardId
            assertThat(streamProxy.getLastProvidedLastSeenShardId())
                    .isEqualTo(shardIds[shardIds.length - 1]);
        }
    }

    @Test
    void testRestoreFromStateRemembersUnassignedSplits() throws Throwable {
        try (MockSplitEnumeratorContext<KinesisShardSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                MockSplitEnumeratorContext<KinesisShardSplit> restoredContext =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS)) {
            TestKinesisStreamProxy streamProxy = getTestStreamProxy();
            final Properties consumerConfig = new Properties();
            KinesisStreamsSourceEnumerator enumerator =
                    new KinesisStreamsSourceEnumerator(
                            context, STREAM_ARN, consumerConfig, streamProxy, null);
            enumerator.start();

            // Given enumerator is initialised with one registered reader, with 4 shards in stream
            final int subtaskId = 1;
            context.registerReader(TestUtil.getTestReaderInfo(subtaskId));
            enumerator.addReader(subtaskId);
            String[] shardIds =
                    new String[] {
                        generateShardId(0),
                        generateShardId(1),
                        generateShardId(2),
                        generateShardId(3)
                    };
            streamProxy.addShards(shardIds);
            // Given enumerator has run discovery
            context.runNextOneTimeCallable();
            // Given one shard split is returned
            SplitsAssignment<KinesisShardSplit> initialSplitAssignment =
                    context.getSplitsAssignmentSequence().get(0);
            KinesisShardSplit returnedSplit =
                    initialSplitAssignment.assignment().get(subtaskId).get(0);
            enumerator.addSplitsBack(ImmutableList.of(returnedSplit), subtaskId);

            // When restored from state
            KinesisStreamsSourceEnumeratorState snapshottedState = enumerator.snapshotState(1);
            KinesisStreamsSourceEnumerator restoredEnumerator =
                    new KinesisStreamsSourceEnumerator(
                            restoredContext,
                            STREAM_ARN,
                            consumerConfig,
                            streamProxy,
                            snapshottedState);
            restoredEnumerator.start();
            // Given enumerator is initialised with one registered reader, with 4 shards in stream
            restoredContext.registerReader(TestUtil.getTestReaderInfo(subtaskId));
            restoredEnumerator.addReader(subtaskId);
            restoredContext.runPeriodicCallable(0);

            // Then returned split will be assigned
            SplitsAssignment<KinesisShardSplit> firstReturnedSplitAssignment =
                    restoredContext.getSplitsAssignmentSequence().get(0);
            assertThat(firstReturnedSplitAssignment.assignment()).containsOnlyKeys(subtaskId);
            assertThat(firstReturnedSplitAssignment.assignment().get(subtaskId))
                    .containsExactly(returnedSplit);
        }
    }

    @Test
    void testRestoreFromStateRemembersCompletedShards() throws Throwable {
        try (MockSplitEnumeratorContext<KinesisShardSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                MockSplitEnumeratorContext<KinesisShardSplit> restoredContext =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS)) {
            TestKinesisStreamProxy streamProxy = getTestStreamProxy();
            final Properties consumerConfig = new Properties();
            KinesisStreamsSourceEnumerator enumerator =
                    new KinesisStreamsSourceEnumerator(
                            context, STREAM_ARN, consumerConfig, streamProxy, null);
            enumerator.start();

            // Given enumerator is initialised with one registered reader, with 4 shards in stream
            final int subtaskId = 1;
            context.registerReader(TestUtil.getTestReaderInfo(subtaskId));
            enumerator.addReader(subtaskId);
            String[] shardIds =
                    new String[] {
                        generateShardId(0),
                        generateShardId(1),
                        generateShardId(2),
                        generateShardId(3)
                    };
            streamProxy.addShards(shardIds);
            // Given also that one shard is marked as complete
            enumerator.handleSourceEvent(
                    subtaskId,
                    new CompletedShardsEvent(
                            ImmutableSet.of(getTestSplit(STREAM_ARN, shardIds[0]).splitId())));

            // When restored from state
            KinesisStreamsSourceEnumeratorState snapshottedState = enumerator.snapshotState(1);
            KinesisStreamsSourceEnumerator restoredEnumerator =
                    new KinesisStreamsSourceEnumerator(
                            restoredContext,
                            STREAM_ARN,
                            consumerConfig,
                            streamProxy,
                            snapshottedState);
            restoredEnumerator.start();
            // Given enumerator is initialised with one registered reader, with 4 shards in stream
            restoredContext.registerReader(TestUtil.getTestReaderInfo(subtaskId));
            restoredEnumerator.addReader(subtaskId);
            restoredContext.runPeriodicCallable(0);

            SplitsAssignment<KinesisShardSplit> initialSplitAssignment =
                    restoredContext.getSplitsAssignmentSequence().get(0);

            // Then all 4 shards discovered on startup
            assertThat(initialSplitAssignment.assignment()).containsOnlyKeys(subtaskId);
            assertThat(
                            initialSplitAssignment.assignment().get(subtaskId).stream()
                                    .map(KinesisShardSplit::getShardId))
                    .containsExactly(Arrays.copyOfRange(shardIds, 1, shardIds.length));
        }
    }

    @Test
    void testHandleUnrecognisedSourceEventIsNoOp() throws Throwable {
        try (MockSplitEnumeratorContext<KinesisShardSplit> context =
                new MockSplitEnumeratorContext<>(NUM_SUBTASKS)) {
            TestKinesisStreamProxy streamProxy = getTestStreamProxy();
            final Properties consumerConfig = new Properties();
            KinesisStreamsSourceEnumerator enumerator =
                    new KinesisStreamsSourceEnumerator(
                            context, STREAM_ARN, consumerConfig, streamProxy, null);

            assertThatNoException()
                    .isThrownBy(() -> enumerator.handleSourceEvent(1, new SourceEvent() {}));
        }
    }

    @Test
    void testCloseIsNoOp() throws Throwable {
        try (MockSplitEnumeratorContext<KinesisShardSplit> context =
                new MockSplitEnumeratorContext<>(NUM_SUBTASKS)) {
            TestKinesisStreamProxy streamProxy = getTestStreamProxy();
            final Properties consumerConfig = new Properties();
            KinesisStreamsSourceEnumerator enumerator =
                    new KinesisStreamsSourceEnumerator(
                            context, STREAM_ARN, consumerConfig, streamProxy, null);
            enumerator.start();

            assertThatNoException().isThrownBy(enumerator::close);
        }
    }

    private KinesisStreamsSourceEnumerator getSimpleEnumeratorWithNoState(
            MockSplitEnumeratorContext<KinesisShardSplit> context, StreamProxy streamProxy) {
        final Properties consumerConfig = new Properties();
        KinesisStreamsSourceEnumerator enumerator =
                new KinesisStreamsSourceEnumerator(
                        context, STREAM_ARN, consumerConfig, streamProxy, null);
        enumerator.start();
        assertThat(context.getOneTimeCallables()).hasSize(1);
        assertThat(context.getPeriodicCallables()).hasSize(1);
        return enumerator;
    }

    private static Stream<Arguments> provideInitialPositions() {
        return Stream.of(
                Arguments.of(InitialPosition.LATEST, "", ShardIteratorType.AT_TIMESTAMP),
                Arguments.of(InitialPosition.TRIM_HORIZON, "", ShardIteratorType.TRIM_HORIZON),
                Arguments.of(
                        InitialPosition.AT_TIMESTAMP,
                        "2023-04-13T09:18:00.0+01:00",
                        ShardIteratorType.AT_TIMESTAMP));
    }
}
