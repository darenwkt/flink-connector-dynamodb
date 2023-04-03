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
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplitState;

import software.amazon.awssdk.services.kinesis.model.Record;

/**
 * TODO: Add JAvadoc.
 *
 * @param <T>
 */
@Internal
public class KinesisStreamsRecordEmitter<T>
        implements RecordEmitter<Record, T, KinesisShardSplitState> {

    private final DeserializationSchema<T> deserializationSchema;

    public KinesisStreamsRecordEmitter(DeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void emitRecord(
            Record element, SourceOutput<T> output, KinesisShardSplitState splitState)
            throws Exception {
        output.collect(
                deserializationSchema.deserialize(element.data().asByteArray()),
                element.approximateArrivalTimestamp().toEpochMilli());
    }
}
