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

import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.List;
import java.util.function.Consumer;

public class IndexInvariantLogReporter implements InvariantReporter<List<CrossCellInconsistency>> {
    private static final SafeLogger log = SafeLoggerFactory.get(IndexInvariantLogReporter.class);

    private final IndexInvariant indexInvariant;

    public IndexInvariantLogReporter(IndexInvariant indexInvariant) {
        this.indexInvariant = indexInvariant;
    }

    @Override
    public Invariant<List<CrossCellInconsistency>> invariant() {
        return indexInvariant;
    }

    @Override
    public Consumer<List<CrossCellInconsistency>> consumer() {
        return violations -> {
            if (!violations.isEmpty()) {
                log.error("Detected rows which did not agree with state in an index: {}",
                        SafeArg.of("violations", violations));
            }
        };
    }
}
