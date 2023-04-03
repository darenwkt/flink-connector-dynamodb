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

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableSet;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.connector.kinesis.source.util.TestUtil.getTestReaderInfo;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.getTestSplit;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

class HashShardAssignerTest {

    @Test
    void testAssignmentIsDeterministic() {
        final KinesisShardSplit split = getTestSplit();
        final KinesisShardAssigner.Context assignerContext = new TestShardAssignerContext();
        assignerContext.getRegisteredReaders().put(1, getTestReaderInfo(1));

        HashShardAssigner assigner = new HashShardAssigner();

        int initiallyAssignedIndex = assigner.assign(split, assignerContext);
        int numberOfAttempts = 1000;

        for (int i = 0; i < numberOfAttempts; i++) {
            assertThat(assigner.assign(split, assignerContext)).isEqualTo(initiallyAssignedIndex);
        }
    }

    @Test
    void testAssignedIndexLimitedToAvailableReaders() {
        final Set<Integer> setOfRegisteredReaders = ImmutableSet.of(42, 53, 64);
        final KinesisShardAssigner.Context assignerContext = new TestShardAssignerContext();
        for (Integer subtaskId : setOfRegisteredReaders) {
            assignerContext.getRegisteredReaders().put(subtaskId, getTestReaderInfo(subtaskId));
        }

        HashShardAssigner assigner = new HashShardAssigner();

        int numberOfAttempts = 1000;
        for (int i = 0; i < numberOfAttempts; i++) {
            assertThat(assigner.assign(getTestSplit(), assignerContext))
                    .isIn(setOfRegisteredReaders);
        }
    }

    @Test
    void testNoRegisteredReadersThrowsException() {
        final KinesisShardAssigner.Context assignerContext = new TestShardAssignerContext();

        HashShardAssigner assigner = new HashShardAssigner();

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> assigner.assign(getTestSplit(), assignerContext))
                .withMessage("Expected at least one registered reader. Unable to assign split.");
    }

    private static class TestShardAssignerContext implements KinesisShardAssigner.Context {
        private final Map<Integer, Set<KinesisShardSplit>> splitAssignment = new HashMap<>();
        private final Map<Integer, ReaderInfo> registeredReaders = new HashMap<>();

        @Override
        public Map<Integer, Set<KinesisShardSplit>> getCurrentSplitAssignment() {
            return splitAssignment;
        }

        @Override
        public Map<Integer, ReaderInfo> getRegisteredReaders() {
            return registeredReaders;
        }
    }
}
