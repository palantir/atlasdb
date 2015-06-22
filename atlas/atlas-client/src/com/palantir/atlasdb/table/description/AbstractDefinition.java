// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.table.description;

import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.CachePriority;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.ExpirationStrategy;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.PartitionStrategy;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.transaction.api.ConflictHandler;

abstract class AbstractDefinition {
    CachePriority cachePriority = CachePriority.WARM;
    PartitionStrategy partitionStrategy = PartitionStrategy.ORDERED;
    ConflictHandler conflictHandler = defaultConflictHandler();
    SweepStrategy sweepStrategy = SweepStrategy.CONSERVATIVE;
    ExpirationStrategy expirationStrategy = ExpirationStrategy.NEVER;

    public void cachePriority(CachePriority priority) {
        this.cachePriority = priority;
    }

    public void partitionStrategy(PartitionStrategy strat) {
        partitionStrategy = strat;
    }

    public void conflictHandler(ConflictHandler handler) {
        conflictHandler = handler;
    }

    public void expirationStrategy(ExpirationStrategy strategy) {
        expirationStrategy = strategy;
    }

    public void sweepStrategy(SweepStrategy strategy) {
        this.sweepStrategy = strategy;
    }

    protected abstract ConflictHandler defaultConflictHandler();
}

