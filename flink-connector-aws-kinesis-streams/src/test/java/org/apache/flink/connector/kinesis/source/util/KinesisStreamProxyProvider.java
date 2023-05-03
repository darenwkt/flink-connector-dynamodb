package org.apache.flink.connector.kinesis.source.util;

import org.apache.flink.connector.kinesis.source.proxy.StreamProxy;
import org.apache.flink.connector.kinesis.source.split.model.StartingPosition;

import org.jetbrains.annotations.Nullable;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.Shard;

import java.util.ArrayList;
import java.util.List;

public class KinesisStreamProxyProvider {

    public static TestKinesisStreamProxy getTestStreamProxy() {
        return new TestKinesisStreamProxy();
    }

    public static class TestKinesisStreamProxy implements StreamProxy {

        private final List<Shard> shards = new ArrayList<>();

        public void addShards(String... shardIds) {
            for (String shardId : shardIds) {
                shards.add(Shard.builder().shardId(shardId).build());
            }
        }

        @Override
        public List<Shard> listShards(String streamArn, @Nullable String lastSeenShardId) {
            List<Shard> results = new ArrayList<>();
            for (Shard shard : shards) {
                if (shard.shardId().equals(lastSeenShardId)) {
                    results.clear();
                    continue;
                }
                results.add(shard);
            }
            return results;
        }

        @Override
        public String getShardIterator(
                String streamArn, String shardId, StartingPosition startingPosition) {
            return null;
        }

        @Override
        public GetRecordsResponse getRecords(
                String streamArn, String shardIterator, final int getMaxNumberOfRecordsPerFetch) {
            return null;
        }
    }
}
