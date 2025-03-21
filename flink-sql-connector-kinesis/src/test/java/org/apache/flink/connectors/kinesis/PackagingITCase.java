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

package org.apache.flink.connectors.kinesis;

import org.apache.flink.packaging.PackagingTestUtils;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.test.resources.ResourceTestUtils;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Arrays;

class PackagingITCase {

    @Test
    void testPackaging() throws Exception {
        final Path jar = ResourceTestUtils.getResource(".*/flink-sql-connector-kinesis[^/]*\\.jar");

        PackagingTestUtils.assertJarContainsOnlyFilesMatching(
                jar,
                Arrays.asList(
                        "org/apache/flink/",
                        "META-INF/",
                        "amazon-kinesis-producer-native-binaries/",
                        "cacerts/",
                        "google/",
                        "LICENSE",
                        "io/",
                        "javax/",
                        ".gitkeep",
                        "software/amazon/",
                        "com/esotericsoftware/",
                        "com/fasterxml/",
                        "com/ibm/",
                        "com/amazonaws/",
                        "com/ibm/",
                        "org/objenesis/",
                        "org/reactivestreams/",
                        "org/slf4j/"));
        PackagingTestUtils.assertJarContainsServiceEntry(jar, Factory.class);
    }
}
