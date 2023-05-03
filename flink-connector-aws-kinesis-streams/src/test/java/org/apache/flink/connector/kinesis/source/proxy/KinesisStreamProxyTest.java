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

import org.apache.flink.connector.kinesis.source.split.model.StartingPosition;
import org.apache.flink.connector.kinesis.source.util.KinesisClientProvider.ListShardItem;
import org.apache.flink.connector.kinesis.source.util.KinesisClientProvider.TestingKinesisClient;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.KinesisRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

import static org.apache.flink.connector.kinesis.source.util.TestUtil.generateShardId;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class KinesisStreamProxyTest {

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"shardId-000000000002"})
    void testListShardsSingleCall(String lastSeenShardId) {
        final String streamArn =
                "arn:aws:kinesis:us-east-1:123456789012:stream/LoadTestBeta_Input_0";
        final List<Shard> expectedShards = getTestShards(0, 3);

        List<ListShardItem> listShardItems =
                ImmutableList.of(
                        ListShardItem.builder()
                                .validation(
                                        getListShardRequestValidation(
                                                streamArn, lastSeenShardId, null))
                                .shards(expectedShards)
                                .nextToken(null)
                                .build());
        TestingKinesisClient testKinesisClient = new TestingKinesisClient(new Properties());
        testKinesisClient.setListShardsResponses(listShardItems);

        KinesisStreamProxy kinesisStreamProxy = new KinesisStreamProxy(testKinesisClient);

        assertThat(kinesisStreamProxy.listShards(streamArn, lastSeenShardId))
                .isEqualTo(expectedShards);
    }

    @Test
    void testListShardsMultipleCalls() {
        final String streamArn =
                "arn:aws:kinesis:us-east-1:123456789012:stream/LoadTestBeta_Input_0";
        final String lastSeenShardId = "shardId-000000000000";
        final List<Shard> expectedShards = getTestShards(0, 3);

        List<ListShardItem> listShardItems =
                ImmutableList.of(
                        ListShardItem.builder()
                                .validation(
                                        getListShardRequestValidation(
                                                streamArn, lastSeenShardId, null))
                                .shards(expectedShards.subList(0, 1))
                                .nextToken("next-token-1")
                                .build(),
                        ListShardItem.builder()
                                .validation(
                                        getListShardRequestValidation(
                                                streamArn, null, "next-token-1"))
                                .shards(expectedShards.subList(1, 2))
                                .nextToken("next-token-2")
                                .build(),
                        ListShardItem.builder()
                                .validation(
                                        getListShardRequestValidation(
                                                streamArn, null, "next-token-2"))
                                .shards(expectedShards.subList(2, 4))
                                .nextToken(null)
                                .build());
        TestingKinesisClient testKinesisClient = new TestingKinesisClient(new Properties());
        testKinesisClient.setListShardsResponses(listShardItems);

        KinesisStreamProxy kinesisStreamProxy = new KinesisStreamProxy(testKinesisClient);

        assertThat(kinesisStreamProxy.listShards(streamArn, lastSeenShardId))
                .isEqualTo(expectedShards);
    }

    @Test
    void testGetShardIteratorTrimHorizon() {
        final String streamArn =
                "arn:aws:kinesis:us-east-1:123456789012:stream/LoadTestBeta_Input_0";
        final String shardId = "shardId-000000000002";
        final StartingPosition startingPosition = StartingPosition.fromStart();
        final String expectedShardIterator = "some-shard-iterator";

        TestingKinesisClient testKinesisClient = new TestingKinesisClient(new Properties());
        testKinesisClient.setNextShardIterator(expectedShardIterator);
        testKinesisClient.setShardIteratorValidation(
                validateEqual(
                        GetShardIteratorRequest.builder()
                                .streamARN(streamArn)
                                .shardId(shardId)
                                .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                                .build()));

        KinesisStreamProxy kinesisStreamProxy = new KinesisStreamProxy(testKinesisClient);

        assertThat(kinesisStreamProxy.getShardIterator(streamArn, shardId, startingPosition))
                .isEqualTo(expectedShardIterator);
    }

    @Test
    void testGetShardIteratorAtTimestamp() {
        final String streamArn =
                "arn:aws:kinesis:us-east-1:123456789012:stream/LoadTestBeta_Input_0";
        final String shardId = "shardId-000000000002";
        final Instant timestamp = Instant.now();
        final StartingPosition startingPosition = StartingPosition.fromTimestamp(timestamp);
        final String expectedShardIterator = "some-shard-iterator";

        TestingKinesisClient testKinesisClient = new TestingKinesisClient(new Properties());
        testKinesisClient.setNextShardIterator(expectedShardIterator);
        testKinesisClient.setShardIteratorValidation(
                validateEqual(
                        GetShardIteratorRequest.builder()
                                .streamARN(streamArn)
                                .shardId(shardId)
                                .shardIteratorType(ShardIteratorType.AT_TIMESTAMP)
                                .timestamp(timestamp)
                                .build()));

        KinesisStreamProxy kinesisStreamProxy = new KinesisStreamProxy(testKinesisClient);

        assertThat(kinesisStreamProxy.getShardIterator(streamArn, shardId, startingPosition))
                .isEqualTo(expectedShardIterator);
    }

    @Test
    void testGetShardIteratorAtSequenceNumber() {
        final String streamArn =
                "arn:aws:kinesis:us-east-1:123456789012:stream/LoadTestBeta_Input_0";
        final String shardId = "shardId-000000000002";
        final String sequenceNumber = "some-sequence-number";
        final StartingPosition startingPosition =
                StartingPosition.continueFromSequenceNumber(sequenceNumber);
        final String expectedShardIterator = "some-shard-iterator";

        TestingKinesisClient testKinesisClient = new TestingKinesisClient(new Properties());
        testKinesisClient.setNextShardIterator(expectedShardIterator);
        testKinesisClient.setShardIteratorValidation(
                validateEqual(
                        GetShardIteratorRequest.builder()
                                .streamARN(streamArn)
                                .shardId(shardId)
                                .shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
                                .startingSequenceNumber(sequenceNumber)
                                .build()));

        KinesisStreamProxy kinesisStreamProxy = new KinesisStreamProxy(testKinesisClient);

        assertThat(kinesisStreamProxy.getShardIterator(streamArn, shardId, startingPosition))
                .isEqualTo(expectedShardIterator);
    }

    @Test
    void testGetRecords() {
        final String streamArn =
                "arn:aws:kinesis:us-east-1:123456789012:stream/LoadTestBeta_Input_0";
        final String shardIterator = "some-shard-iterator";
        final GetRecordsResponse expectedGetRecordsResponse =
                GetRecordsResponse.builder()
                        .records(Record.builder().build())
                        .nextShardIterator("next-iterator")
                        .build();

        TestingKinesisClient testKinesisClient = new TestingKinesisClient(new Properties());
        testKinesisClient.setGetRecordsResponse(expectedGetRecordsResponse);
        testKinesisClient.setGetRecordsValidation(
                validateEqual(
                        GetRecordsRequest.builder()
                                .streamARN(streamArn)
                                .shardIterator(shardIterator)
                                .build()));

        KinesisStreamProxy kinesisStreamProxy = new KinesisStreamProxy(testKinesisClient);

        assertThat(kinesisStreamProxy.getRecords(streamArn, shardIterator, 1))
                .isEqualTo(expectedGetRecordsResponse);
    }

    private List<Shard> getTestShards(final int startShardId, final int endShardId) {
        List<Shard> shards = new ArrayList<>();
        for (int i = startShardId; i <= endShardId; i++) {
            shards.add(Shard.builder().shardId(generateShardId(i)).build());
        }
        return shards;
    }

    private Consumer<ListShardsRequest> getListShardRequestValidation(
            final String streamArn, final String startShardId, final String nextToken) {
        return req -> {
            ListShardsRequest expectedReq =
                    ListShardsRequest.builder()
                            .streamARN(streamArn)
                            .exclusiveStartShardId(startShardId)
                            .nextToken(nextToken)
                            .build();
            assertThat(req).isEqualTo(expectedReq);
        };
    }

    private <R extends KinesisRequest> Consumer<R> validateEqual(final R request) {
        return req -> assertThat(req).isEqualTo(request);
    }
}
