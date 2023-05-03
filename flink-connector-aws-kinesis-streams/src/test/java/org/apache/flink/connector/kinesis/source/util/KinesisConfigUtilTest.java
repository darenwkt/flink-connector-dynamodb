/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kinesis.source.util;

import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.kinesis.source.config.SourceConfigConstants;
import org.apache.flink.connector.kinesis.source.config.SourceConfigUtil;
import org.apache.flink.connector.kinesis.util.KinesisConfigUtil;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import static org.apache.flink.connector.kinesis.source.config.SourceConfigConstants.DEFAULT_STREAM_TIMESTAMP_DATE_FORMAT;
import static org.apache.flink.connector.kinesis.source.config.SourceConfigConstants.STREAM_INITIAL_TIMESTAMP;
import static org.apache.flink.connector.kinesis.source.config.SourceConfigConstants.STREAM_TIMESTAMP_DATE_FORMAT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

/** Tests for KinesisConfigUtil. */
@RunWith(PowerMockRunner.class)
public class KinesisConfigUtilTest {

    @Rule public ExpectedException exception = ExpectedException.none();

    // ----------------------------------------------------------------------
    // validateAwsConfiguration() tests
    // ----------------------------------------------------------------------

    @Test
    public void testUnrecognizableAwsRegionInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Invalid AWS region");

        Properties testConfig = new Properties();
        testConfig.setProperty(AWSConfigConstants.AWS_REGION, "wrongRegionId");
        testConfig.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
        testConfig.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");

        KinesisConfigUtil.validateAwsConfiguration(testConfig);
    }

    @Test
    public void testCredentialProviderTypeSetToBasicButNoCredentialSetInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Please set values for AWS Access Key ID ('"
                        + AWSConfigConstants.AWS_ACCESS_KEY_ID
                        + "') "
                        + "and Secret Key ('"
                        + AWSConfigConstants.AWS_SECRET_ACCESS_KEY
                        + "') when using the BASIC AWS credential provider type.");

        Properties testConfig = new Properties();
        testConfig.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
        testConfig.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");

        KinesisConfigUtil.validateAwsConfiguration(testConfig);
    }

    @Test
    public void testUnrecognizableCredentialProviderTypeInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Invalid AWS Credential Provider Type");

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "wrongProviderType");

        KinesisConfigUtil.validateAwsConfiguration(testConfig);
    }

    // ----------------------------------------------------------------------
    // validateSourceConfiguration() tests
    // ----------------------------------------------------------------------

    @Test
    public void testNoAwsRegionOrEndpointInConsumerConfig() {
        String expectedMessage =
                String.format(
                        "For KinesisStreamsSource AWS region ('%s') must be set in the config.",
                        AWSConfigConstants.AWS_REGION);
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(expectedMessage);

        Properties testConfig = new Properties();
        testConfig.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "accessKey");
        testConfig.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testAwsRegionAndEndpointInConsumerConfig() {
        Properties testConfig = new Properties();
        testConfig.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
        testConfig.setProperty(AWSConfigConstants.AWS_ENDPOINT, "fake");
        testConfig.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "accessKey");
        testConfig.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testAwsRegionInConsumerConfig() {
        Properties testConfig = new Properties();
        testConfig.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
        testConfig.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "accessKey");
        testConfig.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testUnrecognizableStreamInitPositionTypeInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Invalid initial position in stream");

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(SourceConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
        testConfig.setProperty(SourceConfigConstants.STREAM_INITIAL_POSITION, "wrongInitPosition");

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testStreamInitPositionTypeSetToAtTimestampButNoInitTimestampSetInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Please set value for initial timestamp ('"
                        + STREAM_INITIAL_TIMESTAMP
                        + "') when using AT_TIMESTAMP initial position.");

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(SourceConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
        testConfig.setProperty(SourceConfigConstants.STREAM_INITIAL_POSITION, "AT_TIMESTAMP");

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testUnparsableDateforInitialTimestampInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for initial timestamp for AT_TIMESTAMP initial position in stream.");

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(SourceConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
        testConfig.setProperty(SourceConfigConstants.STREAM_INITIAL_POSITION, "AT_TIMESTAMP");
        testConfig.setProperty(STREAM_INITIAL_TIMESTAMP, "unparsableDate");

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testIllegalValuEforInitialTimestampInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for initial timestamp for AT_TIMESTAMP initial position in stream.");

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(SourceConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
        testConfig.setProperty(SourceConfigConstants.STREAM_INITIAL_POSITION, "AT_TIMESTAMP");
        testConfig.setProperty(STREAM_INITIAL_TIMESTAMP, "-1.0");

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testDateStringForValidateOptionDateProperty() {
        String timestamp = "2016-04-04T19:58:46.480-00:00";

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(SourceConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
        testConfig.setProperty(SourceConfigConstants.STREAM_INITIAL_POSITION, "AT_TIMESTAMP");
        testConfig.setProperty(STREAM_INITIAL_TIMESTAMP, timestamp);

        assertThatNoException()
                .isThrownBy(() -> KinesisConfigUtil.validateSourceConfiguration(testConfig));
    }

    @Test
    public void testUnixTimestampForValidateOptionDateProperty() {
        String unixTimestamp = "1459799926.480";

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(SourceConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
        testConfig.setProperty(SourceConfigConstants.STREAM_INITIAL_POSITION, "AT_TIMESTAMP");
        testConfig.setProperty(STREAM_INITIAL_TIMESTAMP, unixTimestamp);

        assertThatNoException()
                .isThrownBy(() -> KinesisConfigUtil.validateSourceConfiguration(testConfig));
    }

    @Test
    public void testInvalidPatternForInitialTimestampInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for initial timestamp for AT_TIMESTAMP initial position in stream.");

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(SourceConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
        testConfig.setProperty(SourceConfigConstants.STREAM_INITIAL_POSITION, "AT_TIMESTAMP");
        testConfig.setProperty(STREAM_INITIAL_TIMESTAMP, "2016-03-14");
        testConfig.setProperty(STREAM_TIMESTAMP_DATE_FORMAT, "InvalidPattern");

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testUnparsableDatEforUserDefinedDatEformatForInitialTimestampInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for initial timestamp for AT_TIMESTAMP initial position in stream.");

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(SourceConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
        testConfig.setProperty(SourceConfigConstants.STREAM_INITIAL_POSITION, "AT_TIMESTAMP");
        testConfig.setProperty(STREAM_INITIAL_TIMESTAMP, "stillUnparsable");
        testConfig.setProperty(STREAM_TIMESTAMP_DATE_FORMAT, "yyyy-MM-dd");

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testDateStringForUserDefinedDatEformatForValidateOptionDateProperty() {
        String unixTimestamp = "2016-04-04";
        String pattern = "yyyy-MM-dd";

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(SourceConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
        testConfig.setProperty(SourceConfigConstants.STREAM_INITIAL_POSITION, "AT_TIMESTAMP");
        testConfig.setProperty(STREAM_INITIAL_TIMESTAMP, unixTimestamp);
        testConfig.setProperty(STREAM_TIMESTAMP_DATE_FORMAT, pattern);

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testUnparsableLongForListShardsBackoffBaseMillisInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for list shards operation base backoff milliseconds");

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(SourceConfigConstants.LIST_SHARDS_BACKOFF_BASE, "unparsableLong");

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testUnparsableLongForListShardsBackoffMaxMillisInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for list shards operation max backoff milliseconds");

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(SourceConfigConstants.LIST_SHARDS_BACKOFF_MAX, "unparsableLong");

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testUnparsableDoublEforListShardsBackoffExponentialConstantInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for list shards operation backoff exponential constant");

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(
                SourceConfigConstants.LIST_SHARDS_BACKOFF_EXPONENTIAL_CONSTANT, "unparsableDouble");

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testUnparsableIntForGetRecordsRetriesInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for maximum retry attempts for getRecords shard operation");

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(SourceConfigConstants.SHARD_GETRECORDS_RETRIES, "unparsableInt");

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testUnparsableIntForGetRecordsMaxCountInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for maximum records per getRecords shard operation");

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(SourceConfigConstants.SHARD_GETRECORDS_MAX, "unparsableInt");

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testUnparsableLongForGetRecordsBackoffBaseMillisInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for get records operation base backoff milliseconds");

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(
                SourceConfigConstants.SHARD_GETRECORDS_BACKOFF_BASE, "unparsableLong");

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testUnparsableLongForGetRecordsBackoffMaxMillisInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for get records operation max backoff milliseconds");

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(
                SourceConfigConstants.SHARD_GETRECORDS_BACKOFF_MAX, "unparsableLong");

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testUnparsableDoublEforGetRecordsBackoffExponentialConstantInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for get records operation backoff exponential constant");

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(
                SourceConfigConstants.SHARD_GETRECORDS_BACKOFF_EXPONENTIAL_CONSTANT,
                "unparsableDouble");

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testUnparsableLongForGetRecordsIntervalMillisInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for getRecords sleep interval in milliseconds");

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(
                SourceConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS, "unparsableLong");

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testUnparsableIntForGetShardIteratorRetriesInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for maximum retry attempts for getShardIterator shard operation");

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(SourceConfigConstants.SHARD_GETITERATOR_RETRIES, "unparsableInt");

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testUnparsableLongForGetShardIteratorBackoffBaseMillisInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for get shard iterator operation base backoff milliseconds");

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(
                SourceConfigConstants.SHARD_GETITERATOR_BACKOFF_BASE, "unparsableLong");

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testUnparsableLongForGetShardIteratorBackoffMaxMillisInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for get shard iterator operation max backoff milliseconds");

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(
                SourceConfigConstants.SHARD_GETITERATOR_BACKOFF_MAX, "unparsableLong");

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testUnparsableDoublEforGetShardIteratorBackoffExponentialConstantInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for get shard iterator operation backoff exponential constant");

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(
                SourceConfigConstants.SHARD_GETITERATOR_BACKOFF_EXPONENTIAL_CONSTANT,
                "unparsableDouble");

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testUnparsableLongForShardDiscoveryIntervalMillisInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for shard discovery sleep interval in milliseconds");

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(
                SourceConfigConstants.SHARD_DISCOVERY_INTERVAL_MILLIS, "unparsableLong");

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testParseStreamTimestampStartingPositionUsingDefaultFormat() throws Exception {
        String timestamp = "2020-08-13T09:18:00.0+01:00";
        Date expectedTimestamp =
                new SimpleDateFormat(DEFAULT_STREAM_TIMESTAMP_DATE_FORMAT).parse(timestamp);

        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(STREAM_INITIAL_TIMESTAMP, timestamp);

        Date actualimestamp =
                SourceConfigUtil.parseStreamTimestampStartingPosition(consumerProperties);

        assertThat(actualimestamp).isEqualTo(expectedTimestamp);
    }

    @Test
    public void testParseStreamTimestampStartingPositionUsingCustomFormat() throws Exception {
        String format = "yyyy-MM-dd'T'HH:mm";
        String timestamp = "2020-08-13T09:23";
        Date expectedTimestamp = new SimpleDateFormat(format).parse(timestamp);

        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(STREAM_INITIAL_TIMESTAMP, timestamp);
        consumerProperties.setProperty(STREAM_TIMESTAMP_DATE_FORMAT, format);

        Date actualimestamp =
                SourceConfigUtil.parseStreamTimestampStartingPosition(consumerProperties);

        assertThat(actualimestamp).isEqualTo(expectedTimestamp);
    }

    @Test
    public void testParseStreamTimestampStartingPositionUsingParseError() {
        exception.expect(NumberFormatException.class);

        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(STREAM_INITIAL_TIMESTAMP, "bad");

        SourceConfigUtil.parseStreamTimestampStartingPosition(consumerProperties);
    }

    @Test
    public void testParseStreamTimestampStartingPositionIllegalArgumentException() {
        exception.expect(IllegalArgumentException.class);

        Properties consumerProperties = new Properties();

        SourceConfigUtil.parseStreamTimestampStartingPosition(consumerProperties);
    }

    public void testUnparsableIntForRegisterStreamRetriesInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for maximum retry attempts for register stream operation");

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(
                SourceConfigConstants.REGISTER_STREAM_CONSUMER_RETRIES, "unparsableInt");

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testUnparsableIntForRegisterStreamTimeoutInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for maximum timeout for register stream consumer. Must be a valid non-negative integer value.");

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(
                SourceConfigConstants.REGISTER_STREAM_CONSUMER_TIMEOUT_SECONDS, "unparsableInt");

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testUnparsableLongForRegisterStreamBackoffBaseMillisInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for register stream operation base backoff milliseconds");

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(
                SourceConfigConstants.REGISTER_STREAM_CONSUMER_BACKOFF_BASE, "unparsableLong");

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testUnparsableLongForRegisterStreamBackoffMaxMillisInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for register stream operation max backoff milliseconds");

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(
                SourceConfigConstants.REGISTER_STREAM_CONSUMER_BACKOFF_MAX, "unparsableLong");

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testUnparsableDoublEforRegisterStreamBackoffExponentialConstantInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for register stream operation backoff exponential constant");

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(
                SourceConfigConstants.REGISTER_STREAM_CONSUMER_BACKOFF_EXPONENTIAL_CONSTANT,
                "unparsableDouble");

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testUnparsableIntForDeRegisterStreamRetriesInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for maximum retry attempts for deregister stream operation");

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(
                SourceConfigConstants.DEREGISTER_STREAM_CONSUMER_RETRIES, "unparsableInt");

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testUnparsableIntForDeRegisterStreamTimeoutInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for maximum timeout for deregister stream consumer. Must be a valid non-negative integer value.");

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(
                SourceConfigConstants.DEREGISTER_STREAM_CONSUMER_TIMEOUT_SECONDS, "unparsableInt");

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testUnparsableLongForDeRegisterStreamBackoffBaseMillisInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for deregister stream operation base backoff milliseconds");

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(
                SourceConfigConstants.DEREGISTER_STREAM_CONSUMER_BACKOFF_BASE, "unparsableLong");

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testUnparsableLongForDeRegisterStreamBackoffMaxMillisInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for deregister stream operation max backoff milliseconds");

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(
                SourceConfigConstants.DEREGISTER_STREAM_CONSUMER_BACKOFF_MAX, "unparsableLong");

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testUnparsableDoublEforDeRegisterStreamBackoffExponentialConstantInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for deregister stream operation backoff exponential constant");

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(
                SourceConfigConstants.DEREGISTER_STREAM_CONSUMER_BACKOFF_EXPONENTIAL_CONSTANT,
                "unparsableDouble");

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testUnparsableIntForDescribeStreamConsumerRetriesInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for maximum retry attempts for describe stream consumer operation");

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(
                SourceConfigConstants.DESCRIBE_STREAM_CONSUMER_RETRIES, "unparsableInt");

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testUnparsableLongForDescribeStreamConsumerBackoffBaseMillisInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for describe stream consumer operation base backoff milliseconds");

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(
                SourceConfigConstants.DESCRIBE_STREAM_CONSUMER_BACKOFF_BASE, "unparsableLong");

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testUnparsableLongForDescribeStreamConsumerBackoffMaxMillisInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for describe stream consumer operation max backoff milliseconds");

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(
                SourceConfigConstants.DESCRIBE_STREAM_CONSUMER_BACKOFF_MAX, "unparsableLong");

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testUnparsableDoublEforDescribeStreamConsumerBackoffExponentialConstantInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for describe stream consumer operation backoff exponential constant");

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(
                SourceConfigConstants.DESCRIBE_STREAM_CONSUMER_BACKOFF_EXPONENTIAL_CONSTANT,
                "unparsableDouble");

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testUnparsableIntForSubscribeToShardRetriesInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for maximum retry attempts for subscribe to shard operation");

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(SourceConfigConstants.SUBSCRIBE_TO_SHARD_RETRIES, "unparsableInt");

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testUnparsableLongForSubscribeToShardBackoffBaseMillisInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for subscribe to shard operation base backoff milliseconds");

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(
                SourceConfigConstants.SUBSCRIBE_TO_SHARD_BACKOFF_BASE, "unparsableLong");

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testUnparsableLongForSubscribeToShardBackoffMaxMillisInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for subscribe to shard operation max backoff milliseconds");

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(
                SourceConfigConstants.SUBSCRIBE_TO_SHARD_BACKOFF_MAX, "unparsableLong");

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testUnparsableDoublEforSubscribeToShardBackoffExponentialConstantInConfig() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Invalid value given for subscribe to shard operation backoff exponential constant");

        Properties testConfig = TestUtil.getStandardProperties();
        testConfig.setProperty(
                SourceConfigConstants.SUBSCRIBE_TO_SHARD_BACKOFF_EXPONENTIAL_CONSTANT,
                "unparsableDouble");

        KinesisConfigUtil.validateSourceConfiguration(testConfig);
    }

    @Test
    public void testGetV2ConsumerClientProperties() {
        Properties properties = new Properties();
        properties.setProperty("retained", "property");

        assertThat(KinesisConfigUtil.getV2ConsumerClientProperties(properties))
                .containsEntry("retained", "property")
                .containsKey("aws.kinesis.client.user-agent-prefix")
                .hasSize(2);
    }
}
