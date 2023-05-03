package org.apache.flink.connector.kinesis.source.enumerator;

import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.connector.kinesis.source.config.SourceConfigConstants;
import org.apache.flink.connector.kinesis.source.config.SourceConfigConstants.InitialPosition;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;
import org.apache.flink.connector.kinesis.source.util.TestUtil;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableSet;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.flink.connector.kinesis.source.util.KinesisStreamProxyProvider.TestKinesisStreamProxy;
import static org.apache.flink.connector.kinesis.source.util.KinesisStreamProxyProvider.getTestStreamProxy;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.generateShardId;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

class KinesisStreamsSourceEnumeratorTest {

    private static final int NUM_SUBTASKS = 5;
    private static final String STREAM_ARN =
            "arn:aws:kinesis:us-east-1:123456789012:stream/keenesesStream";

    @ParameterizedTest
    @MethodSource("provideInitialPositions")
    void testStartWithoutStateDiscoversAndAssignsShards(
            InitialPosition initialPosition, String initialTimestamp) throws Throwable {
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
                    .allSatisfy(s -> assertThat(s).isEqualTo(initialPosition.name()));

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
                    .allSatisfy(s -> assertThat(s).isEqualTo(InitialPosition.TRIM_HORIZON.name()));
        }
    }

    @ParameterizedTest
    @MethodSource("provideInitialPositions")
    void testStartWithStateIgnoresCompletedShards(
            InitialPosition initialPosition, String initialTimestamp) throws Throwable {
        try (MockSplitEnumeratorContext<KinesisShardSplit> context =
                new MockSplitEnumeratorContext<>(NUM_SUBTASKS)) {
            TestKinesisStreamProxy streamProxy = getTestStreamProxy();
            final String completedShard = generateShardId(0);
            final String lastSeenShard = generateShardId(1);
            final Set<String> completedShardIds = ImmutableSet.of(completedShard);

            KinesisStreamsSourceEnumeratorState state =
                    new KinesisStreamsSourceEnumeratorState(completedShardIds, lastSeenShard);

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
                    .allSatisfy(s -> assertThat(s).isEqualTo(InitialPosition.TRIM_HORIZON.name()));
        }
    }

    private static Stream<Arguments> provideInitialPositions() {
        return Stream.of(
                Arguments.of(InitialPosition.LATEST, ""),
                Arguments.of(InitialPosition.TRIM_HORIZON, ""),
                Arguments.of(InitialPosition.AT_TIMESTAMP, "2023-04-13T09:18:00.0+01:00"));
    }
}
