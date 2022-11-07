/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.atomic;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.keyvalue.api.Cell;
import java.util.Set;

public final class MarkAndCasCassImitatingConsensusForgettingStore extends CassandraImitatingConsensusForgettingStore {
    @VisibleForTesting
    static final byte[] IN_PROGRESS_MARKER = new byte[1];

    public MarkAndCasCassImitatingConsensusForgettingStore(double probabilityOfFailure) {
        super(probabilityOfFailure);
    }

    @Override
    public void mark(Cell cell) {
        runAtomically(cell, () -> {
            Set<Node> quorumNodes = getQuorumNodes();
            writeToQuorum(cell, quorumNodes, IN_PROGRESS_MARKER);
        });
    }

    @Override
    public void mark(Set<Cell> cells) {
        // sort by cells to avoid deadlock
        cells.stream().sorted().forEach(this::mark);
    }
}
