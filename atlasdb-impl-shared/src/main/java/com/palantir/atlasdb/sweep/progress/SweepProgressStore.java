/*
 * Copyright 2017 Palantir Technologies
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
package com.palantir.atlasdb.sweep.progress;

import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.api.Transaction;

public abstract class SweepProgressStore {
    final KeyValueService kvs;

    SweepProgressStore(KeyValueService kvs) {
        this.kvs = kvs;
    }

    public static SweepProgressStore create(KeyValueService kvs) {
        return create(kvs, kvs.supportsCheckAndSet());
    }

    @VisibleForTesting
    static SweepProgressStore create(KeyValueService kvs, boolean supportsCas) {
        if (supportsCas) {
            return SweepProgressStoreWithCas.createProgressStore(kvs);
        } else {
            return SweepProgressStoreWithoutCas.createProgressStore(kvs);
        }
    }

    public abstract void clearProgress();
    public abstract void saveProgress(Transaction tx, SweepProgress newProgress);
    public abstract Optional<SweepProgress> loadProgress(Transaction tx);
}
