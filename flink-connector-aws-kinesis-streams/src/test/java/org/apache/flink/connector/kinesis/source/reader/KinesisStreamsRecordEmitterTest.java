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

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplitState;

import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.connector.kinesis.source.util.TestUtil.getTestSplitState;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

class KinesisStreamsRecordEmitterTest {

    private final SimpleStringSchema STRING_SCHEMA = new SimpleStringSchema();

    @Test
    void testEmitRecord() throws Exception {
        final Instant startTime = Instant.now();
        List<Record> inputRecords =
                ImmutableList.of(
                        Record.builder()
                                .data(SdkBytes.fromByteArray(STRING_SCHEMA.serialize("data-1")))
                                .approximateArrivalTimestamp(startTime)
                                .build(),
                        Record.builder()
                                .data(SdkBytes.fromByteArray(STRING_SCHEMA.serialize("data-2")))
                                .approximateArrivalTimestamp(startTime.plusSeconds(10))
                                .build(),
                        Record.builder()
                                .data(SdkBytes.fromByteArray(STRING_SCHEMA.serialize("data-3")))
                                .approximateArrivalTimestamp(startTime.plusSeconds(20))
                                .build());
        final CapturingSourceOutput<String> output = new CapturingSourceOutput<>();
        final KinesisShardSplitState splitState = getTestSplitState();

        KinesisStreamsRecordEmitter<String> emitter =
                new KinesisStreamsRecordEmitter<>(STRING_SCHEMA);
        for (Record record : inputRecords) {
            emitter.emitRecord(record, output, splitState);
        }

        assertThat(output.getEmittedRecords())
                .containsExactlyInAnyOrder("data-1", "data-2", "data-3");
        assertThat(output.getEmittedTimestamps())
                .containsExactlyInAnyOrder(
                        startTime.toEpochMilli(),
                        startTime.plusSeconds(10).toEpochMilli(),
                        startTime.plusSeconds(20).toEpochMilli());
    }

    private static class CapturingSourceOutput<T> implements SourceOutput<T> {

        private final List<T> emittedRecords = new ArrayList<>();
        private final List<Long> emittedTimestamps = new ArrayList<>();

        @Override
        public void collect(T record) {
            emittedRecords.add(record);
        }

        @Override
        public void collect(T record, long timestamp) {
            emittedRecords.add(record);
            emittedTimestamps.add(timestamp);
        }

        @Override
        public void emitWatermark(Watermark watermark) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void markIdle() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void markActive() {
            throw new UnsupportedOperationException();
        }

        public List<T> getEmittedRecords() {
            return emittedRecords;
        }

        public List<Long> getEmittedTimestamps() {
            return emittedTimestamps;
        }
    }
}
