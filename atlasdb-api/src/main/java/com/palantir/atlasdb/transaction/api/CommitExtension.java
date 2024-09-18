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

package com.palantir.atlasdb.transaction.api;

import com.google.common.annotations.Beta;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import java.util.Map;

/**
 * Plugin interface for adding custom behavior to the commit protocol. This is obviously a very powerful tool,
 * so should not really be used by most users. It is intended for advanced use cases where the perf gains of introducing
 * custom behavior outweighs the complexity and additional support burden of doing so.
 */
@Beta
public interface CommitExtension extends AutoCloseable {

    CommitExtension NOOP = new CommitExtension() {};

    default boolean shouldRunCommitProtocol() {
        return false;
    }

    /**
     * Called when we start the commit procedure, before any locks are acquired.
     * Implementations should make sure that any mutable state they have been accumulating
     * is now frozen and will not change during the commit procedure.
     */
    default void onCommitStart() {}

    default void onCommitLocksAcquired(CommitContext _ctx) {}

    /**
     * The passed map is the set of writes accumulated by the transaction.
     * Implementations may choose to add extra writes to commit to the kvs, adding them directly to the map.
     * It's obviously very sharp API, so use with caution.
     *
     * At the point when this is called,
     * the transaction is already holding all the locks on these writes it needs to hold.
     *
     * @param localWrites
     */
    default void prepareWrites(Map<TableReference, ? extends Map<Cell, byte[]>> localWrites) {}

    @Override
    default void close() {}
}
