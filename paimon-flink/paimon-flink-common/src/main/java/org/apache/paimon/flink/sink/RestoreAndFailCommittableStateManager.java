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

import org.apache.paimon.data.serializer.VersionedSerializer;
import org.apache.paimon.flink.FlinkConnectorOptions.FailoverDelayScaling;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.utils.SerializableSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

/**
 * A {@link CommittableStateManager} which stores uncommitted {@link ManifestCommittable}s in state.
 *
 * <p>When the job restarts, these {@link ManifestCommittable}s will be restored and committed, then
 * an intended failure will occur, hoping that after the job restarts, all writers can start writing
 * based on the restored snapshot.
 *
 * <p>Committer subtasks recover independently and there is no barrier between them, so the first
 * subtask to finish recommitting fails the job while its siblings are still recommitting theirs.
 * Those committables are then restored again on the next attempt, where another subtask fails the
 * job in turn, so the number of restarts needed to converge grows with the number of subtasks that
 * had committables to recover. Optionally waiting before the intended failure gives the siblings
 * time to finish, which bounds that to a single restart in the common case. See {@code
 * sink.committer-recovery-failover-delay-per-committable}.
 *
 * <p>Useful for committing snapshots containing records. For example snapshots produced by table
 * store writers.
 */
public class RestoreAndFailCommittableStateManager<GlobalCommitT>
        extends RestoreCommittableStateManager<GlobalCommitT> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG =
            LoggerFactory.getLogger(RestoreAndFailCommittableStateManager.class);

    /** Wait per recommitted committable before the intended failure; zero disables waiting. */
    private final long failoverDelayPerCommittableMillis;

    /** Upper bound for the wait before the intended failure. */
    private final long failoverDelayMaxMillis;

    /** How the wait scales with the number of recommitted committables. */
    private final FailoverDelayScaling failoverDelayScaling;

    public RestoreAndFailCommittableStateManager(
            SerializableSupplier<VersionedSerializer<GlobalCommitT>> committableSerializer,
            boolean partitionMarkDoneRecoverFromState) {
        this(
                committableSerializer,
                partitionMarkDoneRecoverFromState,
                Duration.ZERO,
                Duration.ZERO,
                FailoverDelayScaling.LINEAR);
    }

    public RestoreAndFailCommittableStateManager(
            SerializableSupplier<VersionedSerializer<GlobalCommitT>> committableSerializer,
            boolean partitionMarkDoneRecoverFromState,
            Duration failoverDelayPerCommittable,
            Duration failoverDelayMax) {
        this(
                committableSerializer,
                partitionMarkDoneRecoverFromState,
                failoverDelayPerCommittable,
                failoverDelayMax,
                FailoverDelayScaling.LINEAR);
    }

    public RestoreAndFailCommittableStateManager(
            SerializableSupplier<VersionedSerializer<GlobalCommitT>> committableSerializer,
            boolean partitionMarkDoneRecoverFromState,
            Duration failoverDelayPerCommittable,
            Duration failoverDelayMax,
            FailoverDelayScaling failoverDelayScaling) {
        super(committableSerializer, partitionMarkDoneRecoverFromState);
        this.failoverDelayPerCommittableMillis = failoverDelayPerCommittable.toMillis();
        this.failoverDelayMaxMillis = failoverDelayMax.toMillis();
        this.failoverDelayScaling = failoverDelayScaling;
    }

    @Override
    protected int recover(List<GlobalCommitT> committables, Committer<?, GlobalCommitT> committer)
            throws Exception {
        int numCommitted = super.recover(committables, committer);
        if (numCommitted > 0) {
            long delayMillis = failoverDelayMillis(numCommitted);
            if (delayMillis > 0) {
                LOG.info(
                        "Recommitted {} committables during recovery. Waiting {} ms before the "
                                + "intended failure, so that other committer subtasks can finish "
                                + "recommitting theirs and the job converges in fewer restarts.",
                        numCommitted,
                        delayMillis);
                // Interruption must propagate: the task may be cancelled while waiting. This is
                // safe with respect to correctness because the operator has not finished
                // initializing, so no checkpoint can complete while we wait.
                Thread.sleep(delayMillis);
            }
            throw new RuntimeException(
                    "This exception is intentionally thrown "
                            + "after committing the restored checkpoints. "
                            + "By restarting the job we hope that "
                            + "writers can start writing based on these new commits. "
                            + "Recommitted committables: "
                            + numCommitted
                            + ", waited before failing: "
                            + delayMillis
                            + " ms.");
        }
        return numCommitted;
    }

    /**
     * Wait scaled by the amount of work this subtask just did, as an estimate of how much work its
     * siblings still have left. Committables are distributed across committer subtasks, so a
     * subtask that recommitted a lot is evidence that the others have a lot to recommit too, and a
     * subtask that recommitted nothing does not fail the job at all and never waits.
     *
     * <p>The scaling mode matters at high committer parallelism. Siblings hold a comparable number
     * of committables and start recovering together, so what remains to wait for is the spread
     * between this subtask and the slowest sibling, not the total amount of work -- and that spread
     * grows sublinearly. Every mode is 1x at a single committable, so the configured duration keeps
     * its meaning regardless.
     */
    private long failoverDelayMillis(int numCommitted) {
        if (failoverDelayPerCommittableMillis <= 0 || failoverDelayMaxMillis <= 0) {
            return 0;
        }
        // Compute in double and clamp: the factor is at most numCommitted, so the product stays
        // well inside the range where double represents integers exactly.
        double delayMillis =
                failoverDelayPerCommittableMillis * failoverDelayScaling.factor(numCommitted);
        return delayMillis >= failoverDelayMaxMillis
                ? failoverDelayMaxMillis
                : Math.round(delayMillis);
    }
}
