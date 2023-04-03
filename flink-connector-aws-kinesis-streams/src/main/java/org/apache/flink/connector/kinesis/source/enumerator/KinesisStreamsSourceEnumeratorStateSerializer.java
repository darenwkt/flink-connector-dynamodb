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

package org.apache.flink.connector.kinesis.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/** TODO: Add JAvadoc. */
@Internal
public class KinesisStreamsSourceEnumeratorStateSerializer
        implements SimpleVersionedSerializer<KinesisStreamsSourceEnumeratorState> {

    private static final int CURRENT_VERSION = 0;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(KinesisStreamsSourceEnumeratorState kinesisStreamsSourceEnumeratorState)
            throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {

            boolean hasLastSeenShardId =
                    kinesisStreamsSourceEnumeratorState.getLastSeenShardId() != null;
            out.writeBoolean(hasLastSeenShardId);
            if (hasLastSeenShardId) {
                out.writeUTF(kinesisStreamsSourceEnumeratorState.getLastSeenShardId());
            }

            out.writeInt(kinesisStreamsSourceEnumeratorState.getCompletedShardIds().size());
            for (String shardId : kinesisStreamsSourceEnumeratorState.getCompletedShardIds()) {
                out.writeUTF(shardId);
            }
            out.flush();

            return baos.toByteArray();
        }
    }

    @Override
    public KinesisStreamsSourceEnumeratorState deserialize(
            int version, byte[] serializedEnumeratorState) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serializedEnumeratorState);
                DataInputStream in = new DataInputStream(bais)) {

            String lastSeenShardId = null;

            final boolean hasLastSeenShardId = in.readBoolean();
            if (hasLastSeenShardId) {
                lastSeenShardId = in.readUTF();
            }

            final int numShardIds = in.readInt();
            Set<String> shardIds = new HashSet<>(numShardIds);
            for (int i = 0; i < numShardIds; i++) {
                shardIds.add(in.readUTF());
            }
            if (in.available() > 0) {
                throw new IOException("Unexpected trailing bytes in serialized shardIds");
            }

            return new KinesisStreamsSourceEnumeratorState(shardIds, lastSeenShardId);
        }
    }
}
