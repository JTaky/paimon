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

package org.apache.paimon.flink.sink;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the wait that {@link RestoreAndFailCommittableStateManager} performs before its
 * intended failure.
 */
class RestoreAndFailCommittableStateManagerTest {

    private static final String INTENDED_FAILURE = "This exception is intentionally thrown ";

    @Test
    void testNoWaitByDefault() throws Exception {
        RestoreAndFailCommittableStateManager<String> stateManager =
                new RestoreAndFailCommittableStateManager<>(() -> null, true);

        long start = System.nanoTime();
        assertThatThrownBy(() -> recover(stateManager, 5))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining(INTENDED_FAILURE)
                .hasMessageContaining("waited before failing: 0 ms");
        assertThat(elapsedMillis(start)).isLessThan(1_000L);
    }

    @Test
    void testWaitIsProportionalToCommittedCount() throws Exception {
        RestoreAndFailCommittableStateManager<String> stateManager =
                stateManager(Duration.ofMillis(50), Duration.ofSeconds(10));

        long start = System.nanoTime();
        assertThatThrownBy(() -> recover(stateManager, 6))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining(INTENDED_FAILURE)
                .hasMessageContaining("Recommitted committables: 6")
                .hasMessageContaining("waited before failing: 300 ms");
        assertThat(elapsedMillis(start)).isGreaterThanOrEqualTo(300L);
    }

    @Test
    void testWaitIsCapped() throws Exception {
        RestoreAndFailCommittableStateManager<String> stateManager =
                stateManager(Duration.ofSeconds(1), Duration.ofMillis(200));

        long start = System.nanoTime();
        assertThatThrownBy(() -> recover(stateManager, 1_000))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("waited before failing: 200 ms");
        long elapsed = elapsedMillis(start);
        assertThat(elapsed).isGreaterThanOrEqualTo(200L);
        // Without the cap this would have waited 1000 seconds.
        assertThat(elapsed).isLessThan(30_000L);
    }

    @Test
    void testNoWaitAndNoFailureWhenNothingWasCommitted() throws Exception {
        RestoreAndFailCommittableStateManager<String> stateManager =
                stateManager(Duration.ofSeconds(30), Duration.ofMinutes(10));

        long start = System.nanoTime();
        assertThat(recover(stateManager, 0)).isZero();
        assertThat(elapsedMillis(start)).isLessThan(1_000L);
    }

    @Test
    void testInterruptionDuringWaitIsNotSwallowed() {
        RestoreAndFailCommittableStateManager<String> stateManager =
                stateManager(Duration.ofSeconds(30), Duration.ofMinutes(10));

        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> recover(stateManager, 1))
                    .isInstanceOf(InterruptedException.class);
        } finally {
            // Thread.sleep clears the flag when it throws; clear it again in case it did not.
            Thread.interrupted();
        }
    }

    private static RestoreAndFailCommittableStateManager<String> stateManager(
            Duration perCommittable, Duration max) {
        return new RestoreAndFailCommittableStateManager<>(() -> null, true, perCommittable, max);
    }

    private static int recover(
            RestoreAndFailCommittableStateManager<String> stateManager, int numCommitted)
            throws Exception {
        return stateManager.recover(Collections.emptyList(), new TestCommitter(numCommitted));
    }

    private static long elapsedMillis(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000L;
    }

    /** A {@link Committer} whose {@code filterAndCommit} reports a fixed number of commits. */
    private static class TestCommitter implements Committer<Void, String> {

        private final int numCommitted;

        private TestCommitter(int numCommitted) {
            this.numCommitted = numCommitted;
        }

        @Override
        public int filterAndCommit(
                List<String> globalCommittables,
                boolean checkAppendFiles,
                boolean partitionMarkDoneRecoverFromState) {
            return numCommitted;
        }

        @Override
        public boolean forceCreatingSnapshot() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String combine(long checkpointId, long watermark, List<Void> committables) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String combine(
                long checkpointId, long watermark, String t, List<Void> committables) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void commit(List<String> globalCommittables) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<Long, List<Void>> groupByCheckpoint(Collection<Void> committables) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {}
    }
}
