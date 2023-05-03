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

package org.apache.flink.connector.kinesis.source.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.kinesis.source.proxy.StreamProxy;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplitState;
import org.apache.flink.connector.kinesis.source.split.model.StartingPosition;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.Record;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

/** TODO: Add javadoc. */
@Internal
public class PollingKinesisShardSplitReader implements SplitReader<Record, KinesisShardSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(PollingKinesisShardSplitReader.class);

    private static final RecordsWithSplitIds<Record> EMPTY_RECORDS =
            new KinesisRecordsWithSplitIds(Collections.emptyIterator(), null);

    private final StreamProxy kinesis;
    private final Deque<KinesisShardSplitState> assignedSplits = new ArrayDeque<>();
    private final PollingKinesisShardSplitReaderConfiguration
            pollingKinesisShardSplitReaderConfiguration;

    public PollingKinesisShardSplitReader(
            StreamProxy kinesisProxy, final Properties consumerConfig) {
        this.kinesis = kinesisProxy;
        this.pollingKinesisShardSplitReaderConfiguration =
                new PollingKinesisShardSplitReaderConfiguration(consumerConfig);
    }

    @Override
    public RecordsWithSplitIds<Record> fetch() throws IOException {
        KinesisShardSplitState split = assignedSplits.poll();

        if (split == null) {
            return EMPTY_RECORDS;
        }

        String shardIterator = split.getNextShardIterator();
        split.setNextShardIterator(null);
        if (shardIterator == null) {
            shardIterator =
                    kinesis.getShardIterator(
                            split.getKinesisShardSplit().getStreamArn(),
                            split.getKinesisShardSplit().getShardId(),
                            split.getNextStartingPosition());
        }

        GetRecordsResponse getRecordsResponse = null;
        while (getRecordsResponse == null) {
            try {
                getRecordsResponse =
                        kinesis.getRecords(
                                split.getKinesisShardSplit().getStreamArn(),
                                shardIterator,
                                pollingKinesisShardSplitReaderConfiguration
                                        .getMaxNumberOfRecordsPerFetch());
            } catch (ExpiredIteratorException ex) {
                LOG.warn(
                        "Encountered an unexpected expired iterator {} for shard {};"
                                + " refreshing the iterator ...",
                        shardIterator,
                        split.getKinesisShardSplit().getShardId());

                shardIterator =
                        kinesis.getShardIterator(
                                split.getKinesisShardSplit().getStreamArn(),
                                split.getKinesisShardSplit().getShardId(),
                                split.getNextStartingPosition());

                // sleep for the fetch interval before the next getRecords attempt with the
                // refreshed iterator
                if (pollingKinesisShardSplitReaderConfiguration.getFetchIntervalMillis() != 0) {
                    try {
                        Thread.sleep(
                                pollingKinesisShardSplitReaderConfiguration
                                        .getFetchIntervalMillis());
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        split.setNextShardIterator(getRecordsResponse.nextShardIterator());

        if (!getRecordsResponse.hasRecords() || getRecordsResponse.records().isEmpty()) {
            return EMPTY_RECORDS;
        }

        split.setNextStartingPosition(
                StartingPosition.continueFromSequenceNumber(
                        getRecordsResponse
                                .records()
                                .get(getRecordsResponse.records().size() - 1)
                                .sequenceNumber()));
        return new KinesisRecordsWithSplitIds(
                getRecordsResponse.records().iterator(), split.getKinesisShardSplit().splitId());
    }

    @Override
    public void handleSplitsChanges(SplitsChange<KinesisShardSplit> splitsChanges) {
        for (KinesisShardSplit split : splitsChanges.splits()) {
            assignedSplits.add(new KinesisShardSplitState(split));
        }
    }

    @Override
    public void wakeUp() {
        // Do nothing because we don't have any sleep mechanism
    }

    @Override
    public void close() throws Exception {}

    private static class KinesisRecordsWithSplitIds implements RecordsWithSplitIds<Record> {

        private final Iterator<Record> recordsIterator;
        private final String splitId;

        public KinesisRecordsWithSplitIds(Iterator<Record> recordsIterator, String splitId) {
            this.recordsIterator = recordsIterator;
            this.splitId = splitId;
        }

        @Nullable
        @Override
        public String nextSplit() {
            return recordsIterator.hasNext() ? splitId : null;
        }

        @Nullable
        @Override
        public Record nextRecordFromSplit() {
            return recordsIterator.hasNext() ? recordsIterator.next() : null;
        }

        @Override
        public Set<String> finishedSplits() {
            if (splitId == null) {
                return Collections.emptySet();
            }
            return recordsIterator.hasNext() ? Collections.emptySet() : ImmutableSet.of(splitId);
        }
    }
}
