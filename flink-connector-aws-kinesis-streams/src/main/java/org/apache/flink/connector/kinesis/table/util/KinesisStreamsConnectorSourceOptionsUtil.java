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

package org.apache.flink.connector.kinesis.table.util;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.connector.aws.table.util.AWSOptionUtils;
import org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigUtil;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** Class for handling Kinesis Consumer specific table options. */
@PublicEvolving
public class KinesisStreamsConnectorSourceOptionsUtil extends AWSOptionUtils {
    private final Map<String, String> resolvedOptions;
    private final String streamArn;
    /**
     * Prefix for properties defined in {
     * org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants} that are
     * delegated to { org.apache.flink.streaming.connectors.kinesis.source.KinesisStreamsSource}.
     */
    public static final String CONSUMER_PREFIX = "scan.";

    public KinesisStreamsConnectorSourceOptionsUtil(
            Map<String, String> resolvedOptions, String streamArn) {
        super(resolvedOptions);
        this.resolvedOptions = resolvedOptions;
        this.streamArn = streamArn;
    }

    @Override
    public Map<String, String> getProcessedResolvedOptions() {
        Map<String, String> mappedResolvedOptions = super.getProcessedResolvedOptions();
        for (String key : resolvedOptions.keySet()) {
            if (key.startsWith(CONSUMER_PREFIX)) {
                mappedResolvedOptions.put(translateConsumerKey(key), resolvedOptions.get(key));
            }
        }
        return mappedResolvedOptions;
    }

    @Override
    public List<String> getNonValidatedPrefixes() {
        return Arrays.asList(AWS_PROPERTIES_PREFIX, CONSUMER_PREFIX);
    }

    public Configuration getValidatedSourceConfigurations() {
        Configuration sourceConfig = Configuration.fromMap(this.getProcessedResolvedOptions());
        sourceConfig.addAll(
                ConfigurationUtils.createConfiguration(super.getValidatedConfigurations()));

        KinesisStreamsSourceConfigUtil.validateStreamSourceConfiguration(sourceConfig);

        return sourceConfig;
    }

    /** Map {@code scan.foo.bar} to {@code flink.foo.bar}. */
    private static String translateConsumerKey(String key) {
        String result = "flink." + key.substring(CONSUMER_PREFIX.length());

        if (result.endsWith("initpos-timestamp-format")) {
            return result.replace("initpos-timestamp-format", "initpos.timestamp.format");
        } else if (result.endsWith("initpos-timestamp")) {
            return result.replace("initpos-timestamp", "initpos.timestamp");
        } else {
            return result;
        }
    }
}
