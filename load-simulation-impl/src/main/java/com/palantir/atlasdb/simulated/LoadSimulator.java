/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.simulated;

import java.util.concurrent.ExecutorService;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.simulated.config.LoadSimulatorConfig;
import com.palantir.atlasdb.simulation.LoadSimulation;
import com.palantir.atlasdb.transaction.api.ReplayRepetition;
import com.palantir.atlasdb.transaction.api.SamplingTransactionCondition;
import com.palantir.atlasdb.transaction.api.StaticReplayRepetition;
import com.palantir.atlasdb.transaction.api.TransactionTaskCondition;
import com.palantir.atlasdb.transaction.impl.RefreshableReplayRepetition;
import com.palantir.atlasdb.transaction.impl.RefreshableTransactionTaskCondition;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.atlasdb.transaction.impl.SuccessfulTaskInvocationCapture;
import com.palantir.atlasdb.transaction.impl.TransactionReplayer;
import com.palantir.atlasdb.transaction.impl.WrappingSerializableTransactionManager;
import com.palantir.common.concurrent.PTExecutors;

public class LoadSimulator implements LoadSimulation, AutoCloseable {
    private final RefreshableTransactionTaskCondition captureCondition = new RefreshableTransactionTaskCondition(
            SamplingTransactionCondition.NEVER_SAMPLE
    );
    private final RefreshableReplayRepetition replayRepetition = new RefreshableReplayRepetition(
            StaticReplayRepetition.NO_REPETITIONS
    );
    private final LoadSimulatorConfig config;
    private final ExecutorService executor;

    public LoadSimulator(LoadSimulatorConfig config) {
        Preconditions.checkState(config.enabled(), "LoadSimulatorConfig is not enabled");
        this.config = config;
        this.executor = PTExecutors.newFixedThreadPool(config.executorThreads());
    }

    @Override
    public void capture(TransactionTaskCondition condition) {
        captureCondition.refresh(condition);
    }

    @Override
    public void replay(ReplayRepetition repetition) {
        replayRepetition.refresh(repetition);
    }

    public SerializableTransactionManager wrap(SerializableTransactionManager delegate) {
        return new WrappingSerializableTransactionManager(
                delegate,
                new SuccessfulTaskInvocationCapture(
                        captureCondition,
                        new TransactionReplayer(
                                executor,
                                delegate,
                                replayRepetition
                        )
                )
        );
    }

    @Override
    public void close() throws Exception {
        executor.shutdown();
    }
}
