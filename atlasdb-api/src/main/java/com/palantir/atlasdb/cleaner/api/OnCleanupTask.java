/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.cleaner.api;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.transaction.api.Transaction;
import java.util.Set;

/**
 * This task allows us to essentially implement ON DELETE CASCADE.
 * <p>
 * If we have an index that marks usage of a shared resource effectively reference counting
 * this resource, then this task can be used to see if all references are gone and remove
 * the resource that is no longer needed.
 * <p>
 * The original intent of this was to clean up shared streams when doing a hard delete
 * of media that used these shared streams.
 */
public interface OnCleanupTask {
    /**
     * This method is run in a fresh transaction before a cell is cleaned up. This method has
     * an opportunity to use the fresh transaction to make decisions about what else needs
     * to be cleaned up.
     * <p>
     * This method may be called for uncommited cells or old cells with new values.
     * <p>
     * If the current transaction fills up this method should return true and it will be called
     * again with a fresh transaction.  This is useful if a large cleanup is needed.
     *
     * @return true if this method should be called again with a fresh transaction
     */
    boolean cellsCleanedUp(Transaction transaction, Set<Cell> cells);
}
