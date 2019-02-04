/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.restore;

import com.palantir.timestamp.TimestampRange;

/**
 * A {@link TransactionTableRangeDeleter} deletes ranges of entries from a transaction table. It is typically used as
 * a part of deleting inconsistent transactions when restoring from a database that doesn't support PITR backup
 * semantics.
 */
public interface TransactionTableRangeDeleter {
    /**
     * For a given transactions table, deletes all entries that have commit timestamp in the provided range.
     *
     * Warning: This is likely to be an expensive operation as it may, on some implementations, result in a full table
     * scan of the underlying transactions table.
     *
     * @param commitTimestampRange range of commit timestamps which we want to delete
     */
    void deleteRange(TimestampRange commitTimestampRange);
}
