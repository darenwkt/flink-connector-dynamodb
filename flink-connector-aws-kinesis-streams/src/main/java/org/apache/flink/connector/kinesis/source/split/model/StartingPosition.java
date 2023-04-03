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

package org.apache.flink.connector.kinesis.source.split.model;

import org.apache.flink.annotation.Internal;

import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

import javax.annotation.Nullable;

import java.time.Instant;

import static software.amazon.awssdk.services.kinesis.model.ShardIteratorType.AFTER_SEQUENCE_NUMBER;
import static software.amazon.awssdk.services.kinesis.model.ShardIteratorType.AT_TIMESTAMP;
import static software.amazon.awssdk.services.kinesis.model.ShardIteratorType.TRIM_HORIZON;

@Internal
public class StartingPosition {

    private final ShardIteratorType shardIteratorType;
    private final Object startingMarker;

    private StartingPosition(ShardIteratorType shardIteratorType, Object startingMarker) {
        this.shardIteratorType = shardIteratorType;
        this.startingMarker = startingMarker;
    }

    public ShardIteratorType getShardIteratorType() {
        return shardIteratorType;
    }

    @Nullable
    public Object getStartingMarker() {
        return startingMarker;
    }

    public static StartingPosition fromTimestamp(final Instant timestamp) {
        return new StartingPosition(AT_TIMESTAMP, timestamp);
    }

    public static StartingPosition continueFromSequenceNumber(final String sequenceNumber) {
        return new StartingPosition(AFTER_SEQUENCE_NUMBER, sequenceNumber);
    }

    public static StartingPosition fromStart() {
        return new StartingPosition(TRIM_HORIZON, null);
    }
}
