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

import org.apache.flink.connector.kinesis.source.proxy.KinesisClientProxy;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.Shard;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

public class KinesisClientProvider {

    public static class TestingKinesisClient extends KinesisClientProxy {

        private Deque<ListShardItem> listShardQueue = new ArrayDeque<>();
        private String shardIterator;
        private Consumer<GetShardIteratorRequest> getShardIteratorValidation;
        private GetRecordsResponse getRecordsResponse;
        private Consumer<GetRecordsRequest> getRecordsValidation;

        public TestingKinesisClient(Properties consumerConfig) {
            super(consumerConfig);
        }

        public String serviceName() {
            return "kinesis";
        }

        public void close() {}

        public void setNextShardIterator(String shardIterator) {
            this.shardIterator = shardIterator;
        }

        public void setShardIteratorValidation(Consumer<GetShardIteratorRequest> validation) {
            this.getShardIteratorValidation = validation;
        }

        @Override
        public GetShardIteratorResponse getShardIterator(
                GetShardIteratorRequest getShardIteratorRequest)
                throws AwsServiceException, SdkClientException {
            getShardIteratorValidation.accept(getShardIteratorRequest);
            return GetShardIteratorResponse.builder().shardIterator(shardIterator).build();
        }

        public void setListShardsResponses(List<ListShardItem> items) {
            listShardQueue.addAll(items);
        }

        @Override
        public ListShardsResponse listShards(ListShardsRequest listShardsRequest)
                throws AwsServiceException, SdkClientException {
            ListShardItem item = listShardQueue.pop();

            item.validation.accept(listShardsRequest);
            return ListShardsResponse.builder()
                    .shards(item.shards)
                    .nextToken(item.nextToken)
                    .build();
        }

        public void setGetRecordsResponse(GetRecordsResponse getRecordsResponse) {
            this.getRecordsResponse = getRecordsResponse;
        }

        public void setGetRecordsValidation(Consumer<GetRecordsRequest> validation) {
            this.getRecordsValidation = validation;
        }

        @Override
        public GetRecordsResponse getRecords(GetRecordsRequest getRecordsRequest)
                throws AwsServiceException, SdkClientException {
            getRecordsValidation.accept(getRecordsRequest);
            return getRecordsResponse;
        }
    }

    public static class ListShardItem {
        private final Consumer<ListShardsRequest> validation;
        private final List<Shard> shards;
        private final String nextToken;

        private ListShardItem(
                Consumer<ListShardsRequest> validation, List<Shard> shards, String nextToken) {
            this.validation = validation;
            this.shards = shards;
            this.nextToken = nextToken;
        }

        public static ListShardItem.Builder builder() {
            return new ListShardItem.Builder();
        }

        public static class Builder {
            private Consumer<ListShardsRequest> validation;
            private List<Shard> shards;
            private String nextToken;

            public Builder validation(Consumer<ListShardsRequest> validation) {
                this.validation = validation;
                return this;
            }

            public Builder shards(List<Shard> shards) {
                this.shards = shards;
                return this;
            }

            public Builder nextToken(String nextToken) {
                this.nextToken = nextToken;
                return this;
            }

            public KinesisClientProvider.ListShardItem build() {
                return new KinesisClientProvider.ListShardItem(validation, shards, nextToken);
            }
        }
    }
}
