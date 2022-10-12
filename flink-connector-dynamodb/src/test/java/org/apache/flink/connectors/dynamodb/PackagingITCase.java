package org.apache.flink.connectors.dynamodb;

import org.apache.flink.packaging.PackagingTestUtils;
import org.apache.flink.test.resources.ResourceTestUtils;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Arrays;

/** Test to verify contents of packaged jar. */
public class PackagingITCase {
    @Test
    void testPackaging() throws Exception {
        final Path jar = ResourceTestUtils.getResource(".*/flink-connector-dynamodb-[^/]*\\.jar");

        PackagingTestUtils.assertJarContainsOnlyFilesMatching(jar, Arrays.asList("META-INF/"));
    }
}
