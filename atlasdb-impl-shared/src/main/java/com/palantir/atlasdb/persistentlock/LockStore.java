/**
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
package com.palantir.atlasdb.persistentlock;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;

public final class LockStore {
    private final KeyValueService keyValueService;

    private LockStore(KeyValueService kvs) {
        this.keyValueService = kvs;
    }

    public static LockStore create(KeyValueService kvs) {
        kvs.createTable(AtlasDbConstants.PERSISTED_LOCKS_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
        return new LockStore(kvs);
    }

    public LockEntry acquireLock(String reason) {
        LockEntry lockEntry = ImmutableLockEntry.builder().lockId(1).reason(reason).build();

        keyValueService.putUnlessExists(AtlasDbConstants.PERSISTED_LOCKS_TABLE, lockEntry.insertionMap());

        return lockEntry;
    }
}
