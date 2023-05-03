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

package org.apache.flink.connector.kinesis.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.kinesis.source.enumerator.KinesisStreamsSourceEnumerator;
import org.apache.flink.connector.kinesis.source.enumerator.KinesisStreamsSourceEnumeratorState;
import org.apache.flink.connector.kinesis.source.enumerator.KinesisStreamsSourceEnumeratorStateSerializer;
import org.apache.flink.connector.kinesis.source.proxy.KinesisClientProxy;
import org.apache.flink.connector.kinesis.source.proxy.KinesisStreamProxy;
import org.apache.flink.connector.kinesis.source.reader.KinesisStreamsRecordEmitter;
import org.apache.flink.connector.kinesis.source.reader.PollingKinesisShardSplitReader;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplitSerializer;
import org.apache.flink.connector.kinesis.util.KinesisConfigUtil;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import software.amazon.awssdk.services.kinesis.model.Record;

import java.util.Properties;
import java.util.function.Supplier;

@PublicEvolving
public class KinesisStreamsSource<T>
        implements Source<T, KinesisShardSplit, KinesisStreamsSourceEnumeratorState> {

    private final String streamArn;
    private final Properties consumerConfig;
    private final DeserializationSchema<T> deserializationSchema;

    public KinesisStreamsSource(
            String streamArn,
            Properties consumerConfig,
            DeserializationSchema<T> deserializationSchema) {
        this.streamArn = streamArn;

        KinesisConfigUtil.validateSourceConfiguration(consumerConfig);
        this.consumerConfig = consumerConfig;
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<T, KinesisShardSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        FutureCompletingBlockingQueue<RecordsWithSplitIds<Record>> elementsQueue =
                new FutureCompletingBlockingQueue<>();
        KinesisStreamProxy kinesisStreamProxy = createKinesisStreamProxy(consumerConfig);
        Supplier<PollingKinesisShardSplitReader> splitReaderSupplier =
                () -> new PollingKinesisShardSplitReader(kinesisStreamProxy, consumerConfig);
        KinesisStreamsRecordEmitter<T> recordEmitter =
                new KinesisStreamsRecordEmitter<>(deserializationSchema);

        return new KinesisStreamsSourceReader<>(
                elementsQueue,
                new SingleThreadFetcherManager<>(elementsQueue, splitReaderSupplier::get),
                recordEmitter,
                toConfiguration(consumerConfig),
                readerContext);
    }

    @Override
    public SplitEnumerator<KinesisShardSplit, KinesisStreamsSourceEnumeratorState> createEnumerator(
            SplitEnumeratorContext<KinesisShardSplit> enumContext) throws Exception {
        return restoreEnumerator(enumContext, null);
    }

    @Override
    public SplitEnumerator<KinesisShardSplit, KinesisStreamsSourceEnumeratorState>
            restoreEnumerator(
                    SplitEnumeratorContext<KinesisShardSplit> enumContext,
                    KinesisStreamsSourceEnumeratorState checkpoint)
                    throws Exception {
        return new KinesisStreamsSourceEnumerator(
                enumContext,
                streamArn,
                consumerConfig,
                createKinesisStreamProxy(consumerConfig),
                checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<KinesisShardSplit> getSplitSerializer() {
        return new KinesisShardSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<KinesisStreamsSourceEnumeratorState>
            getEnumeratorCheckpointSerializer() {
        return new KinesisStreamsSourceEnumeratorStateSerializer();
    }

    private Configuration toConfiguration(Properties props) {
        Configuration config = new Configuration();
        props.stringPropertyNames().forEach(key -> config.setString(key, props.getProperty(key)));
        return config;
    }

    private KinesisStreamProxy createKinesisStreamProxy(Properties consumerConfig) {
        return new KinesisStreamProxy(new KinesisClientProxy(consumerConfig));
    }
}
