/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.timelock.paxos;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosValue;

public class BatchingPaxosLearnerFactory {

    private final PaxosComponents paxosComponents;
    private final DisruptorAutobatcher<Map.Entry<Client, PaxosValue>, Void> learnAutobatcher;
    private final DisruptorAutobatcher<WithSeq<Client>, PaxosValue> getLearnedValueAutobatcher;
    private final DisruptorAutobatcher<WithSeq<Client>, Collection<PaxosValue>> getLearnedValuesSinceAutobatcher;

    private BatchingPaxosLearnerFactory(
            PaxosComponents paxosComponents,
            DisruptorAutobatcher<Map.Entry<Client, PaxosValue>, Void> learnAutobatcher,
            DisruptorAutobatcher<WithSeq<Client>, PaxosValue> getLearnedValueAutobatcher,
            DisruptorAutobatcher<WithSeq<Client>, Collection<PaxosValue>> getLearnedValuesSinceAutobatcher) {
        this.paxosComponents = paxosComponents;
        this.learnAutobatcher = learnAutobatcher;
        this.getLearnedValueAutobatcher = getLearnedValueAutobatcher;
        this.getLearnedValuesSinceAutobatcher = getLearnedValuesSinceAutobatcher;
    }

    public static BatchingPaxosLearnerFactory create(BatchPaxosLearner batchPaxosLearner, PaxosComponents components) {
        DisruptorAutobatcher<Map.Entry<Client, PaxosValue>, Void> learnAutobatcher =
                Autobatchers.coalescing(new LearnCoalescingConsumer(batchPaxosLearner))
                        .safeLoggablePurpose("batch-paxos-learner.learn")
                        .build();

        DisruptorAutobatcher<WithSeq<Client>, PaxosValue> learnedValuesAutobatcher =
                Autobatchers.coalescing(new LearnedValuesCoalescingFunction(batchPaxosLearner))
                        .safeLoggablePurpose("batch-paxos-learner.learned-values")
                        .build();

        DisruptorAutobatcher<WithSeq<Client>, Collection<PaxosValue>> learnedValuesSinceAutobatcher =
                Autobatchers.coalescing(new LearnedValuesSinceCoalescingFunction(batchPaxosLearner))
                .safeLoggablePurpose("batch-paxos-learner.learned-values-since")
                .build();

        return new BatchingPaxosLearnerFactory(
                components,
                learnAutobatcher,
                learnedValuesAutobatcher,
                learnedValuesSinceAutobatcher);
    }

    public PaxosLearner paxosLearnerForClient(Client client, boolean isLocal) {
        return new BatchingPaxosLearner(client, isLocal);
    }

    private final class BatchingPaxosLearner implements PaxosLearner {
        private final Client client;
        private final boolean isLocal;

        private BatchingPaxosLearner(Client client, boolean isLocal) {
            this.client = client;
            this.isLocal = isLocal;
        }

        @Override
        public void learn(long seq, PaxosValue val) {
            try {
                Preconditions.checkArgument(seq == val.getRound(), "seq differs from PaxosValue.round");
                learnAutobatcher.apply(Maps.immutableEntry(client, val)).get();
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                }
                throw new RuntimeException(cause);
            } catch (InterruptedException e) {
                // TODO(fdesouza): handle
                throw new RuntimeException(e);
            }
        }

        @Nullable
        @Override
        public PaxosValue getLearnedValue(long seq) {
            try {
                return getLearnedValueAutobatcher.apply(WithSeq.of(seq, client)).get();
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                }
                throw new RuntimeException(cause);
            } catch (InterruptedException e) {
                // TODO(fdesouza): handle
                throw new RuntimeException(e);
            }
        }

        @Nullable
        @Override
        public PaxosValue getGreatestLearnedValue() {
            if (isLocal) {
                return paxosComponents.learner(client).getGreatestLearnedValue();
            } else {
                throw new UnsupportedOperationException();
            }
        }

        @Nonnull
        @Override
        public Collection<PaxosValue> getLearnedValuesSince(long seq) {
            try {
                return getLearnedValuesSinceAutobatcher.apply(WithSeq.of(seq, client)).get();
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                }
                throw new RuntimeException(cause);
            } catch (InterruptedException e) {
                // TODO(fdesouza): handle
                throw new RuntimeException(e);
            }
        }
    }
}
