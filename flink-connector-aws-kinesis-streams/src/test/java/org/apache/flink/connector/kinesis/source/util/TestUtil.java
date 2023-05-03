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

package org.apache.flink.connector.kinesis.source.util;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.kinesis.source.config.SourceConfigConstants;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplitState;

import java.time.Instant;
import java.util.Properties;

public class TestUtil {

    public static final String STREAM_ARN =
            "arn:aws:kinesis:us-east-1:123456789012:stream/keenesesStream";
    public static final String SHARD_ID = "shardId-000000000002";

    public static String generateShardId(int shardId) {
        return String.format("shardId-%012d", shardId);
    }

    public static KinesisShardSplitState getTestSplitState() {
        return new KinesisShardSplitState(getTestSplit());
    }

    public static KinesisShardSplit getTestSplit() {
        return new KinesisShardSplit(
                STREAM_ARN,
                SHARD_ID,
                SourceConfigConstants.InitialPosition.LATEST.toString(),
                Instant.now().toEpochMilli());
    }

    public static ReaderInfo getTestReaderInfo(final int subtaskId) {
        return new ReaderInfo(subtaskId, "some-location");
    }

    /** Get standard Kinesis-related config properties. */
    public static Properties getStandardProperties() {
        Properties config = new Properties();
        config.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
        config.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
        config.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");

        return config;
    }
}
