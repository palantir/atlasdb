/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.queue;

import com.palantir.atlasdb.keyvalue.api.WriteReference;
import com.palantir.atlasdb.transaction.api.Mutability;
import com.palantir.atlasdb.transaction.api.TableMutabilityArbitrator;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;

public class MutabilityTargetedSweepFilter implements TargetedSweepFilter {
    private final TableMutabilityArbitrator mutabilityArbitrator;

    public MutabilityTargetedSweepFilter(TableMutabilityArbitrator mutabilityArbitrator) {
        this.mutabilityArbitrator = mutabilityArbitrator;
    }

    @Override
    public Collection<WriteInfo> filter(Collection<WriteInfo> cellsToDelete) {
        return cellsToDelete.stream()
                .filter(writeInfo -> {
                    Optional<WriteReference> maybeWriteReference = writeInfo.writeRef();
                    if (maybeWriteReference.isEmpty()) {
                        return true;
                    }
                    WriteReference writeReference = maybeWriteReference.get();
                    Mutability mutability = mutabilityArbitrator.getMutability(writeReference.tableRef());

                    // If the table is at least weak immutable, and we are dealing with a non-tombstone value, there
                    // is no need to apply a range tombstone. This is because the definitions of how these tables are
                    // constructed mean that there can only be one write of a real value. Also, cleaning up of aborted
                    // values happens earlier in the process.
                    return !(mutability.isAtLeastWeakImmutable() && !writeReference.isTombstone());
                })
                .collect(Collectors.toList());
    }
}
