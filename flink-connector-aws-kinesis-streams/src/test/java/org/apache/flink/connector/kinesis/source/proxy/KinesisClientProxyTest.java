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

package org.apache.flink.connector.kinesis.source.proxy;

import org.apache.flink.connector.aws.config.AWSConfigConstants;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.core.client.config.SdkClientConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.utils.AttributeMap;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import static org.apache.flink.connector.kinesis.source.config.SourceConfigConstants.DEFAULT_LIST_SHARDS_BACKOFF_BASE;
import static org.apache.flink.connector.kinesis.source.config.SourceConfigConstants.DEFAULT_LIST_SHARDS_BACKOFF_MAX;
import static org.apache.flink.connector.kinesis.source.config.SourceConfigConstants.DEFAULT_LIST_SHARDS_RETRIES;
import static org.apache.flink.connector.kinesis.source.config.SourceConfigConstants.DEFAULT_SHARD_GETITERATOR_BACKOFF_BASE;
import static org.apache.flink.connector.kinesis.source.config.SourceConfigConstants.DEFAULT_SHARD_GETITERATOR_BACKOFF_MAX;
import static org.apache.flink.connector.kinesis.source.config.SourceConfigConstants.DEFAULT_SHARD_GETITERATOR_RETRIES;
import static org.apache.flink.connector.kinesis.source.config.SourceConfigConstants.DEFAULT_SHARD_GETRECORDS_BACKOFF_BASE;
import static org.apache.flink.connector.kinesis.source.config.SourceConfigConstants.DEFAULT_SHARD_GETRECORDS_BACKOFF_MAX;
import static org.apache.flink.connector.kinesis.source.config.SourceConfigConstants.DEFAULT_SHARD_GETRECORDS_RETRIES;

class KinesisClientProxyTest {

    @ParameterizedTest
    @MethodSource("testClientProxyRetryConfig")
    void testConsumerConfigDefaults(
            ClientProxy.Endpoint endpoint,
            int expectedNumRetries,
            long expectedBaseDelay,
            long expectedMaxBackOffTime)
            throws Exception {
        Properties properties = properties();

        KinesisClientProxy clientProxy = new KinesisClientProxy(properties);

        KinesisClient kinesisClient = clientProxy.getOrCreateKinesisClient(endpoint);

        SdkClientConfiguration conf = getField("clientConfiguration", kinesisClient);
        AttributeMap attributeMap = getField("attributes", conf);
        Map<Object, Object> attributes = getField("attributes", attributeMap);

        attributes.values().stream()
                .forEach(
                        obj -> {
                            if (obj instanceof RetryPolicy) {
                                RetryPolicy retryPolicy = (RetryPolicy) obj;
                                Assertions.assertThat(
                                        retryPolicy.numRetries().equals(expectedNumRetries));
                                Assertions.assertThat(
                                        retryPolicy.backoffStrategy()
                                                instanceof FullJitterBackoffStrategy);
                                FullJitterBackoffStrategy fullJitterBackoffStrategy =
                                        (FullJitterBackoffStrategy) retryPolicy.backoffStrategy();
                                Assertions.assertThat(
                                        fullJitterBackoffStrategy
                                                .toBuilder()
                                                .baseDelay()
                                                .equals(expectedBaseDelay));
                                Assertions.assertThat(
                                        fullJitterBackoffStrategy
                                                .toBuilder()
                                                .maxBackoffTime()
                                                .equals(expectedMaxBackOffTime));
                            }
                        });
    }

    private static Stream<Arguments> testClientProxyRetryConfig() {
        return Stream.of(
                Arguments.of(
                        ClientProxy.Endpoint.LIST_SHARDS,
                        DEFAULT_LIST_SHARDS_RETRIES,
                        DEFAULT_LIST_SHARDS_BACKOFF_BASE,
                        DEFAULT_LIST_SHARDS_BACKOFF_MAX),
                Arguments.of(
                        ClientProxy.Endpoint.GET_RECORDS,
                        DEFAULT_SHARD_GETRECORDS_RETRIES,
                        DEFAULT_SHARD_GETRECORDS_BACKOFF_BASE,
                        DEFAULT_SHARD_GETRECORDS_BACKOFF_MAX),
                Arguments.of(
                        ClientProxy.Endpoint.GET_SHARDS_ITERATOR,
                        DEFAULT_SHARD_GETITERATOR_RETRIES,
                        DEFAULT_SHARD_GETITERATOR_BACKOFF_BASE,
                        DEFAULT_SHARD_GETITERATOR_BACKOFF_MAX));
    }

    private <T> T getField(String fieldName, Object obj) throws Exception {
        Field field = obj.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return (T) field.get(obj);
    }

    private Properties properties() {
        Properties properties = new Properties();
        properties.setProperty(AWSConfigConstants.AWS_REGION, "eu-west-2");
        return properties;
    }
}
