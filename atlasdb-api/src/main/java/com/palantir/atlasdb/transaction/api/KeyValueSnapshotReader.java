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

import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import java.util.Map;
import java.util.Set;

/**
 * Reads state of a {@link com.palantir.atlasdb.keyvalue.api.KeyValueService} in accordance with the provided
 * AtlasDB timestamp, following the AtlasDB read protocol. This includes reading the most recent committed value for
 * each cell that would be visible at the provided timestamp(s) and filtering out versions that have been aborted or
 * not committed yet.
 * <p>
 * If used in the context of a transaction, users are responsible for validating that snapshots read are still
 * guaranteed to be consistent (for example, transactions may need to validate their pre-commit conditions or check
 * that sweep has not progressed). Some methods may have partial, intermediate validation required as part of servicing
 * a read; this class will carry out this intermediate validation.
 * <p>
 * Although this interface performs user-level reads, internal writes may be performed (for example, as part of the
 * read protocol, to abort a long-running transaction).
 */
public interface KeyValueSnapshotReader {
    ListenableFuture<Map<Cell, byte[]>> getAsync(TableReference tableReference, Set<Cell> cells);
}
