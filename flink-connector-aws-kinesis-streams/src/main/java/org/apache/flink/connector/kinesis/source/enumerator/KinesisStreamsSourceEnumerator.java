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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.connector.kinesis.source.config.SourceConfigConstants.InitialPosition;
import org.apache.flink.connector.kinesis.source.proxy.StreamProxy;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.Shard;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.connector.kinesis.source.config.SourceConfigConstants.DEFAULT_STREAM_INITIAL_POSITION;
import static org.apache.flink.connector.kinesis.source.config.SourceConfigConstants.STREAM_INITIAL_POSITION;
import static org.apache.flink.connector.kinesis.source.config.SourceConfigUtil.parseStreamTimestampStartingPosition;

@PublicEvolving
public class KinesisStreamsSourceEnumerator
        implements SplitEnumerator<KinesisShardSplit, KinesisStreamsSourceEnumeratorState> {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisStreamsSourceEnumerator.class);

    private final SplitEnumeratorContext<KinesisShardSplit> context;
    private final String streamArn;
    private final Properties consumerConfig;
    private final StreamProxy streamProxy;
    private final KinesisShardAssigner shardAssigner;
    private final KinesisShardAssigner.Context shardAssignerContext;

    private final Map<Integer, Set<KinesisShardSplit>> splitAssignment = new HashMap<>();
    private final Set<KinesisShardSplit> assignedSplits = new HashSet<>();
    private final Set<String> completedShards;

    private String lastSeenShardId;

    public KinesisStreamsSourceEnumerator(
            SplitEnumeratorContext<KinesisShardSplit> context,
            String streamArn,
            Properties consumerConfig,
            StreamProxy streamProxy,
            KinesisStreamsSourceEnumeratorState state) {
        this.context = context;
        this.streamArn = streamArn;
        this.consumerConfig = consumerConfig;
        this.streamProxy = streamProxy;
        this.shardAssigner = new HashShardAssigner();
        this.shardAssignerContext =
                new ShardAssignerContext(splitAssignment, context.registeredReaders());
        if (state == null) {
            this.completedShards = new HashSet<>();
            this.lastSeenShardId = null;
        } else {
            this.completedShards = state.getCompletedShardIds();
            this.lastSeenShardId = state.getLastSeenShardId();
        }
    }

    @Override
    public void start() {
        if (lastSeenShardId == null) {
            context.callAsync(this::initialDiscoverSplits, this::assignSplits);
        }

        context.callAsync(this::periodicallyDiscoverSplits, this::assignSplits, 10_000L, 10_000L);
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        // Do nothing, since we assign splits eagerly
    }

    @Override
    public void addSplitsBack(List<KinesisShardSplit> splits, int subtaskId) {
        if (!splitAssignment.containsKey(subtaskId)) {
            LOG.warn(
                    "Unable to add splits back for subtask {} since it is not assigned any splits. Splits: {}",
                    subtaskId,
                    splits);
            return;
        }

        for (KinesisShardSplit split : splits) {
            splitAssignment.get(subtaskId).remove(split);
            assignedSplits.remove(split);
        }
    }

    @Override
    public void addReader(int subtaskId) {
        splitAssignment.putIfAbsent(subtaskId, new HashSet<>());
    }

    @Override
    public KinesisStreamsSourceEnumeratorState snapshotState(long checkpointId) throws Exception {
        return new KinesisStreamsSourceEnumeratorState(completedShards, lastSeenShardId);
    }

    @Override
    public void close() throws IOException {}

    private List<KinesisShardSplit> initialDiscoverSplits() {
        List<Shard> shards = streamProxy.listShards(streamArn, lastSeenShardId);
        return mapToSplits(shards, false);
    }

    private List<KinesisShardSplit> periodicallyDiscoverSplits() {
        List<Shard> shards = streamProxy.listShards(streamArn, lastSeenShardId);
        // Any shard discovered after the initial startup should be read from the start, since they
        // come from resharding
        return mapToSplits(shards, true);
    }

    private List<KinesisShardSplit> mapToSplits(List<Shard> shards, boolean shouldReadFromStart) {
        InitialPosition startingPosition =
                shouldReadFromStart
                        ? InitialPosition.TRIM_HORIZON
                        : InitialPosition.valueOf(
                                consumerConfig
                                        .getOrDefault(
                                                STREAM_INITIAL_POSITION,
                                                DEFAULT_STREAM_INITIAL_POSITION)
                                        .toString());
        long startingTimestamp = 0;
        switch (startingPosition) {
            case LATEST:
                startingTimestamp = Instant.now().toEpochMilli();
                break;
            case AT_TIMESTAMP:
                startingTimestamp = parseStreamTimestampStartingPosition(consumerConfig).getTime();
                break;
            case TRIM_HORIZON:
            default:
                // Since we are reading from TRIM_HORIZON, starting time epoch millis can be 0
        }

        List<KinesisShardSplit> splits = new ArrayList<>();
        for (Shard shard : shards) {
            splits.add(
                    new KinesisShardSplit(
                            streamArn,
                            shard.shardId(),
                            startingPosition.toString(),
                            startingTimestamp));
        }

        return splits;
    }

    private void assignSplits(List<KinesisShardSplit> discoveredSplits, Throwable t) {
        if (t != null) {
            throw new FlinkRuntimeException("Failed to list shards.", t);
        }

        if (!discoveredSplits.isEmpty()) {
            KinesisShardSplit lastSplit = discoveredSplits.get(discoveredSplits.size() - 1);
            lastSeenShardId = lastSplit.getShardId();
        }

        Map<Integer, List<KinesisShardSplit>> newSplitAssignments = new HashMap<>();

        for (KinesisShardSplit split : discoveredSplits) {
            if (completedShards.contains(split.getShardId()) || assignedSplits.contains(split)) {
                continue;
            }

            int selectedSubtask = shardAssigner.assign(split, shardAssignerContext);
            if (newSplitAssignments.containsKey(selectedSubtask)) {
                newSplitAssignments.get(selectedSubtask).add(split);
            } else {
                List<KinesisShardSplit> subtaskList = new ArrayList<>();
                subtaskList.add(split);
                newSplitAssignments.put(selectedSubtask, subtaskList);
            }

            assignedSplits.add(split);
            splitAssignment.get(selectedSubtask).add(split);
        }
        context.assignSplits(new SplitsAssignment<>(newSplitAssignments));
    }

    @Internal
    private static class ShardAssignerContext implements KinesisShardAssigner.Context {

        private final Map<Integer, Set<KinesisShardSplit>> splitAssignment;
        private final Map<Integer, ReaderInfo> registeredReaders;

        private ShardAssignerContext(
                Map<Integer, Set<KinesisShardSplit>> splitAssignment,
                Map<Integer, ReaderInfo> registeredReaders) {
            this.splitAssignment = splitAssignment;
            this.registeredReaders = registeredReaders;
        }

        @Override
        public Map<Integer, Set<KinesisShardSplit>> getCurrentSplitAssignment() {
            return splitAssignment;
        }

        public Map<Integer, ReaderInfo> getRegisteredReaders() {
            return registeredReaders;
        }
    }
}
