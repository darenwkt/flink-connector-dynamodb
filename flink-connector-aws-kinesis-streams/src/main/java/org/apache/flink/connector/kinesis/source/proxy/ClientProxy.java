package org.apache.flink.connector.kinesis.source.proxy;

import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;

public interface ClientProxy {
    ListShardsResponse listShards(ListShardsRequest listShardsRequest);

    GetRecordsResponse getRecords(GetRecordsRequest getRecordsRequest);

    GetShardIteratorResponse getShardIterator(GetShardIteratorRequest getShardIteratorRequest);

    enum Endpoint {
        LIST_SHARDS,
        GET_SHARDS_ITERATOR,
        GET_RECORDS
    }
}
