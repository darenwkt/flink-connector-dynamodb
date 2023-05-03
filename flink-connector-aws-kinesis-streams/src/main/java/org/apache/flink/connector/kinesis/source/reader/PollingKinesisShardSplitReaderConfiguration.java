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
import org.apache.flink.connector.kinesis.source.config.SourceConfigConstants;

import java.util.Properties;

/** TODO: Add javadoc. */
@Internal
public class PollingKinesisShardSplitReaderConfiguration {

    private final boolean adaptiveReads;

    private final int maxNumberOfRecordsPerFetch;

    private final long fetchIntervalMillis;

    public PollingKinesisShardSplitReaderConfiguration(final Properties consumerConfig) {
        this.maxNumberOfRecordsPerFetch =
                Integer.parseInt(
                        consumerConfig.getProperty(
                                SourceConfigConstants.SHARD_GETRECORDS_MAX,
                                Integer.toString(
                                        SourceConfigConstants.DEFAULT_SHARD_GETRECORDS_MAX)));

        this.fetchIntervalMillis =
                Long.parseLong(
                        consumerConfig.getProperty(
                                SourceConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS,
                                Long.toString(
                                        SourceConfigConstants
                                                .DEFAULT_SHARD_GETRECORDS_INTERVAL_MILLIS)));

        this.adaptiveReads =
                Boolean.parseBoolean(
                        consumerConfig.getProperty(
                                SourceConfigConstants.SHARD_USE_ADAPTIVE_READS,
                                Boolean.toString(
                                        SourceConfigConstants.DEFAULT_SHARD_USE_ADAPTIVE_READS)));
    }

    public boolean isAdaptiveReads() {
        return adaptiveReads;
    }

    public int getMaxNumberOfRecordsPerFetch() {
        return maxNumberOfRecordsPerFetch;
    }

    public long getFetchIntervalMillis() {
        return fetchIntervalMillis;
    }
}
