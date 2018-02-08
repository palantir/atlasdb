/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.external;

import java.util.Set;

import com.palantir.atlasdb.transaction.api.Transaction;

public interface GenericStreamDeletionFilter {
    /**
     * Filters down a set of stream identifiers to a subset of said identifiers that are eligible for deletion
     * from the key-value service.
     *
     * @param tx An open Atlas transaction, which may be useful for retrieving information to guide deletion decisions
     * @param identifiers Stream identifiers that are to be considered for deletion
     * @return Set of stream identifiers that should be deleted from the database; guaranteed to be a subset
     *         of the provided identifiers
     */
    Set<GenericStreamIdentifier> getStreamIdentifiersToDelete(Transaction tx, Set<GenericStreamIdentifier> identifiers);
}
