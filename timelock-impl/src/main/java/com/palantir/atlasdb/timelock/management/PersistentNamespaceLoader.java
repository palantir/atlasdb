/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.management;

import com.palantir.paxos.Client;
import java.util.Set;

public interface PersistentNamespaceLoader {
    /**
     * Gets all namespaces that have been persisted (via the persistence method under question).
     * No transactionality guarantees are given: namespace additions and deletions while the
     * request is running may or may not be reflected in the output.
     */
    Set<Client> getAllPersistedNamespaces();
}
