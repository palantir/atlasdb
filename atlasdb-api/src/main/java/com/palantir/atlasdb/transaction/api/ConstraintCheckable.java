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
package com.palantir.atlasdb.transaction.api;

import com.palantir.atlasdb.keyvalue.api.Cell;
import java.util.List;
import java.util.Map;

public interface ConstraintCheckable {
    /**
     * This method checks row, table, and foreign-key constraints on the values given in writes.
     * This method may require additional reads from the database, depending on the value of
     * constraintCheckingMode. This is intended to be used to check constraints on writes.
     */
    List<String> findConstraintFailures(
            Map<Cell, byte[]> writes,
            ConstraintCheckingTransaction transaction,
            AtlasDbConstraintCheckingMode constraintCheckingMode);

    /**
     * Calling this method will not cause any read calls from the database. It assumes that all necessary
     * information for constraint checking is given in writes. For dynamic tables, any constraint between
     * columns of a single row will probably require writes to have all columns for each given row. This
     * is intended to be used for garbage collection (where all values are being read).
     */
    List<String> findConstraintFailuresNoRead(
            Map<Cell, byte[]> reads, AtlasDbConstraintCheckingMode constraintCheckingMode);
}
