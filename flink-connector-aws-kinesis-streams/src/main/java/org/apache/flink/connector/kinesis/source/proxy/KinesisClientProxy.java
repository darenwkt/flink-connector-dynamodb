package org.apache.flink.connector.kinesis.source.proxy;

import org.apache.flink.connector.aws.util.AWSClientUtil;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.connector.kinesis.sink.KinesisStreamsConfigConstants;
import org.apache.flink.connector.kinesis.source.config.SourceConfigConstants;
import org.apache.flink.connector.kinesis.util.KinesisConfigUtil;
import org.apache.flink.util.Preconditions;

import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.utils.AttributeMap;

import java.time.Duration;
import java.util.Optional;
import java.util.Properties;

import static software.amazon.awssdk.http.SdkHttpConfigurationOption.TCP_KEEPALIVE;

public class KinesisClientProxy implements ClientProxy {

    private static KinesisClient kinesisClientListShards = null;
    private static KinesisClient kinesisClientGetShardsIterator = null;
    private static KinesisClient kinesisClientGetRecords = null;
    private static KinesisClient defaultKinesisClient = null;
    private static Properties consumerConfig;

    // ------------------------------------------------------------------------
    //  listShards() related performance settings
    // ------------------------------------------------------------------------

    /** Base backoff millis for the list shards operation. */
    private static long listShardsBaseBackoffMillis;

    /** Maximum backoff millis for the list shards operation. */
    private static long listShardsMaxBackoffMillis;

    /** Maximum retry attempts for the list shards operation. */
    private static int listShardsMaxRetries;

    // ------------------------------------------------------------------------
    //  getRecords() related performance settings
    // ------------------------------------------------------------------------

    /** Base backoff millis for the get records operation. */
    private static long getRecordsBaseBackoffMillis;

    /** Maximum backoff millis for the get records operation. */
    private static long getRecordsMaxBackoffMillis;

    /** Maximum retry attempts for the get records operation. */
    private static int getRecordsMaxRetries;

    // ------------------------------------------------------------------------
    //  getShardIterator() related performance settings
    // ------------------------------------------------------------------------

    /** Base backoff millis for the get shard iterator operation. */
    private static long getShardIteratorBaseBackoffMillis;

    /** Maximum backoff millis for the get shard iterator operation. */
    private static long getShardIteratorMaxBackoffMillis;

    /** Maximum retry attempts for the get shard iterator operation. */
    private static int getShardIteratorMaxRetries;

    /** Backoff millis for the describe stream operation. */
    private static long describeStreamBaseBackoffMillis;

    /** Maximum backoff millis for the describe stream operation. */
    private static long describeStreamMaxBackoffMillis;

    public KinesisClientProxy(Properties consumerConfig) {
        KinesisClientProxy.consumerConfig = consumerConfig;

        listShardsBaseBackoffMillis =
                Long.parseLong(
                        consumerConfig.getProperty(
                                SourceConfigConstants.LIST_SHARDS_BACKOFF_BASE,
                                Long.toString(
                                        SourceConfigConstants.DEFAULT_LIST_SHARDS_BACKOFF_BASE)));
        listShardsMaxBackoffMillis =
                Long.parseLong(
                        consumerConfig.getProperty(
                                SourceConfigConstants.LIST_SHARDS_BACKOFF_MAX,
                                Long.toString(
                                        SourceConfigConstants.DEFAULT_LIST_SHARDS_BACKOFF_MAX)));
        listShardsMaxRetries =
                Integer.parseInt(
                        consumerConfig.getProperty(
                                SourceConfigConstants.LIST_SHARDS_RETRIES,
                                Long.toString(SourceConfigConstants.DEFAULT_LIST_SHARDS_RETRIES)));
        describeStreamBaseBackoffMillis =
                Long.parseLong(
                        consumerConfig.getProperty(
                                SourceConfigConstants.STREAM_DESCRIBE_BACKOFF_BASE,
                                Long.toString(
                                        SourceConfigConstants
                                                .DEFAULT_STREAM_DESCRIBE_BACKOFF_BASE)));
        describeStreamMaxBackoffMillis =
                Long.parseLong(
                        consumerConfig.getProperty(
                                SourceConfigConstants.STREAM_DESCRIBE_BACKOFF_MAX,
                                Long.toString(
                                        SourceConfigConstants
                                                .DEFAULT_STREAM_DESCRIBE_BACKOFF_MAX)));
        getRecordsBaseBackoffMillis =
                Long.parseLong(
                        consumerConfig.getProperty(
                                SourceConfigConstants.SHARD_GETRECORDS_BACKOFF_BASE,
                                Long.toString(
                                        SourceConfigConstants
                                                .DEFAULT_SHARD_GETRECORDS_BACKOFF_BASE)));
        getRecordsMaxBackoffMillis =
                Long.parseLong(
                        consumerConfig.getProperty(
                                SourceConfigConstants.SHARD_GETRECORDS_BACKOFF_MAX,
                                Long.toString(
                                        SourceConfigConstants
                                                .DEFAULT_SHARD_GETRECORDS_BACKOFF_MAX)));
        getRecordsMaxRetries =
                Integer.parseInt(
                        consumerConfig.getProperty(
                                SourceConfigConstants.SHARD_GETRECORDS_RETRIES,
                                Long.toString(
                                        SourceConfigConstants.DEFAULT_SHARD_GETRECORDS_RETRIES)));
        getShardIteratorBaseBackoffMillis =
                Long.parseLong(
                        consumerConfig.getProperty(
                                SourceConfigConstants.SHARD_GETITERATOR_BACKOFF_BASE,
                                Long.toString(
                                        SourceConfigConstants
                                                .DEFAULT_SHARD_GETITERATOR_BACKOFF_BASE)));
        getShardIteratorMaxBackoffMillis =
                Long.parseLong(
                        consumerConfig.getProperty(
                                SourceConfigConstants.SHARD_GETITERATOR_BACKOFF_MAX,
                                Long.toString(
                                        SourceConfigConstants
                                                .DEFAULT_SHARD_GETITERATOR_BACKOFF_MAX)));
        getShardIteratorMaxRetries =
                Integer.parseInt(
                        consumerConfig.getProperty(
                                SourceConfigConstants.SHARD_GETITERATOR_RETRIES,
                                Long.toString(
                                        SourceConfigConstants.DEFAULT_SHARD_GETITERATOR_RETRIES)));
    }

    public KinesisClient getOrCreateKinesisClient(Endpoint endpoint) {
        switch (endpoint) {
            case GET_RECORDS:
                if (kinesisClientGetRecords == null) {
                    return createKinesisClient(endpoint);
                }
                return kinesisClientGetRecords;
            case LIST_SHARDS:
                if (kinesisClientListShards == null) {
                    return createKinesisClient(endpoint);
                }
                return kinesisClientListShards;
            case GET_SHARDS_ITERATOR:
                if (kinesisClientGetShardsIterator == null) {
                    return createKinesisClient(endpoint);
                }
                return kinesisClientGetShardsIterator;
            default:
                if (defaultKinesisClient == null) {
                    return createKinesisClient(endpoint);
                }
                return defaultKinesisClient;
        }
    }

    private KinesisClient createKinesisClient(Endpoint endpoint) {
        Preconditions.checkNotNull(consumerConfig);

        final AttributeMap.Builder clientConfiguration = AttributeMap.builder();
        populateDefaultValues(clientConfiguration);

        final SdkHttpClient httpClient =
                AWSGeneralUtil.createSyncHttpClient(
                        clientConfiguration.build(), ApacheHttpClient.builder());

        Properties clientProperties =
                KinesisConfigUtil.getV2ConsumerClientProperties(consumerConfig);

        AWSGeneralUtil.validateAwsCredentials(consumerConfig);

        return AWSClientUtil.createAwsSyncClient(
                clientProperties,
                httpClient,
                KinesisClient.builder(),
                KinesisStreamsConfigConstants.BASE_KINESIS_USER_AGENT_PREFIX_FORMAT,
                KinesisStreamsConfigConstants.KINESIS_CLIENT_USER_AGENT_PREFIX,
                getRetryPolicy(endpoint));
    }

    private Optional<RetryPolicy> getRetryPolicy(Endpoint endpoint) {
        switch (endpoint) {
            case GET_RECORDS:
                return Optional.of(
                        RetryPolicy.builder()
                                .backoffStrategy(
                                        FullJitterBackoffStrategy.builder()
                                                .baseDelay(
                                                        Duration.ofMillis(
                                                                getRecordsBaseBackoffMillis))
                                                .maxBackoffTime(
                                                        Duration.ofMillis(
                                                                getRecordsMaxBackoffMillis))
                                                .build())
                                .numRetries(getRecordsMaxRetries)
                                .build());
            case LIST_SHARDS:
                return Optional.of(
                        RetryPolicy.builder()
                                .backoffStrategy(
                                        FullJitterBackoffStrategy.builder()
                                                .baseDelay(
                                                        Duration.ofMillis(
                                                                listShardsBaseBackoffMillis))
                                                .maxBackoffTime(
                                                        Duration.ofMillis(
                                                                listShardsMaxBackoffMillis))
                                                .build())
                                .numRetries(listShardsMaxRetries)
                                .build());
            case GET_SHARDS_ITERATOR:
                return Optional.of(
                        RetryPolicy.builder()
                                .backoffStrategy(
                                        FullJitterBackoffStrategy.builder()
                                                .baseDelay(
                                                        Duration.ofMillis(
                                                                getShardIteratorBaseBackoffMillis))
                                                .maxBackoffTime(
                                                        Duration.ofMillis(
                                                                getShardIteratorMaxBackoffMillis))
                                                .build())
                                .numRetries(getShardIteratorMaxRetries)
                                .build());
        }
        return Optional.empty();
    }

    @Override
    public ListShardsResponse listShards(ListShardsRequest listShardsRequest) {
        return getOrCreateKinesisClient(Endpoint.LIST_SHARDS).listShards(listShardsRequest);
    }

    @Override
    public GetRecordsResponse getRecords(GetRecordsRequest getRecordsRequest) {
        return getOrCreateKinesisClient(Endpoint.GET_RECORDS).getRecords(getRecordsRequest);
    }

    @Override
    public GetShardIteratorResponse getShardIterator(
            GetShardIteratorRequest getShardIteratorRequest) {
        return getOrCreateKinesisClient(Endpoint.GET_SHARDS_ITERATOR)
                .getShardIterator(getShardIteratorRequest);
    }

    private static void populateDefaultValues(final AttributeMap.Builder clientConfiguration) {
        clientConfiguration.put(TCP_KEEPALIVE, true);
    }
}
