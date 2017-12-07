/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.priority;

import java.util.Collection;
import java.util.List;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.Transaction;

public interface SweepPriorityStore {
    void delete(Transaction tx, Collection<TableReference> tableRefs);
    void update(Transaction tx, TableReference tableRef, UpdateSweepPriority update);
    List<SweepPriority> loadNewPriorities(Transaction tx);
    List<SweepPriority> loadOldPriorities(Transaction tx, long sweepTimestamp);

    default boolean isInitialized() {
        return true;
    }
}
