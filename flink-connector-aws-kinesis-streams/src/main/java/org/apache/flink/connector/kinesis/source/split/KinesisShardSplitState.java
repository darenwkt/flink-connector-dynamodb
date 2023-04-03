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

package org.apache.flink.connector.kinesis.source.split;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.kinesis.source.config.SourceConfigConstants;
import org.apache.flink.connector.kinesis.source.split.model.StartingPosition;

import java.time.Instant;

/** TODO: Add javadoc. */
@Internal
public class KinesisShardSplitState {
    private final KinesisShardSplit kinesisShardSplit;
    private StartingPosition nextStartingPosition;
    private transient String nextShardIterator;

    public KinesisShardSplitState(KinesisShardSplit kinesisShardSplit) {
        this.kinesisShardSplit = kinesisShardSplit;
        this.nextStartingPosition = parseStartingPosition(kinesisShardSplit);
    }

    public KinesisShardSplit getKinesisShardSplit() {
        return kinesisShardSplit;
    }

    public StartingPosition getNextStartingPosition() {
        return nextStartingPosition;
    }

    public void setNextStartingPosition(StartingPosition nextStartingPosition) {
        this.nextStartingPosition = nextStartingPosition;
    }

    public String getNextShardIterator() {
        return nextShardIterator;
    }

    public void setNextShardIterator(String nextShardIterator) {
        this.nextShardIterator = nextShardIterator;
    }

    private StartingPosition parseStartingPosition(KinesisShardSplit split) {
        SourceConfigConstants.InitialPosition initialPosition =
                SourceConfigConstants.InitialPosition.valueOf(split.getStartingPosition());
        switch (initialPosition) {
            case LATEST:
            case AT_TIMESTAMP:
                return StartingPosition.fromTimestamp(
                        Instant.ofEpochMilli(split.getStartingTimestamp()));
            case TRIM_HORIZON:
                return StartingPosition.fromStart();
            default:
                throw new IllegalArgumentException(
                        "Unsupported starting position when initializing KinesisShardSplitState. Starting position: "
                                + split.getStartingPosition());
        }
    }
}
