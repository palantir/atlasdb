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

package com.palantir.atlasdb.namespacecleaner;

import java.io.Closeable;

/**
 * Allows for deleting all data from a namespace (e.g., dropping the keyspace for a Cassandra KVS) programmatically,
 * and determining when that cleanup is complete.
 */
public interface NamespaceCleaner extends Closeable {
    /**
     * Deletes all data associated with a specific namespace. In Cassandra, this is done by dropping the keyspace.
     *
     * Deleting all data is naturally dangerous, and care must be taken to avoid inadvertently deleting a namespace
     * that is still in active use.
     *
     * It is safe to retry this method if cleanup fails.
     */
    void deleteAllDataFromNamespace();

    /**
     * Returns true if all the data for this namespace has been deleted successfully.
     */
    boolean isNamespaceDeletedSuccessfully();
}
