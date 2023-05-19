/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.workload.invariant;

import com.palantir.atlasdb.workload.transaction.witnessed.InvalidWitnessedTransaction;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.List;
import java.util.function.Consumer;

public enum SerializableInvariantLogReporter implements InvariantReporter<List<InvalidWitnessedTransaction>> {
    INSTANCE;
    private static final SafeLogger log = SafeLoggerFactory.get(SerializableInvariantLogReporter.class);

    @Override
    public Invariant<List<InvalidWitnessedTransaction>> invariant() {
        return SerializableInvariant.INSTANCE;
    }

    @Override
    public Consumer<List<InvalidWitnessedTransaction>> consumer() {
        return invalidWitnessedTransactions -> {
            if (!invalidWitnessedTransactions.isEmpty()) {
                log.error(
                        "Detected transactions that violated our serializable isolation invariant {}",
                        SafeArg.of("invalidWitnessedTransactions", invalidWitnessedTransactions));
            }
        };
    }
}
