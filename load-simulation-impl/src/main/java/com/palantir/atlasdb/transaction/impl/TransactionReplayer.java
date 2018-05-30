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

package com.palantir.atlasdb.transaction.impl;

import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.transaction.api.CapturedTransaction;
import com.palantir.atlasdb.transaction.api.ConditionAwareTransactionManager;
import com.palantir.atlasdb.transaction.api.DeliberatelyFailedNonRetriableException;
import com.palantir.atlasdb.transaction.api.ReplayRepetition;

public class TransactionReplayer implements Consumer<CapturedTransaction> {
    private static final Logger log = LoggerFactory.getLogger(TransactionReplayer.class);

    private final ExecutorService executor;
    private final ReplayRepetition repetition;
    private final ConditionAwareTransactionManager manager;

    public TransactionReplayer(ExecutorService executor, ConditionAwareTransactionManager manager,
            ReplayRepetition repetition) {
        this.executor = executor;
        this.manager = manager;
        this.repetition = repetition;
    }

    @Override
    public void accept(CapturedTransaction captured) {
        int repetitions = repetition.repetitions(captured);
        log.debug("repeating captured transaction with {} timestamp {} times.", captured.timestamp(), repetitions);
        accept(captured, repetitions);
    }

    private void accept(CapturedTransaction captured, int repetitions) {
        if (repetitions > 0) {
            executor.submit(() -> {
                try {
                    log.trace("running captured transaction with {} timestamp; {} repetitions remaining",
                            captured.timestamp(), repetitions - 1);
                    manager.runTaskWithConditionThrowOnConflict(new AlwaysFailingReplayCondition(), captured);
                } catch (DeliberatelyFailedNonRetriableException exception) {
                    // Ignore exceptions we expect to be occurring.
                }
                log.trace("successfully ran captured transaction with {} timestamp.", captured.timestamp());
                accept(captured, repetitions - 1);
            });
        }
    }
}
