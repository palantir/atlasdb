/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.table.description;

import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.transaction.api.ConflictHandler;

public final class ConflictHandlers {
    private ConflictHandlers() {
        // Utility
    }

    public static TableMetadataPersistence.TableConflictHandler persistToProto(ConflictHandler conflictHandler) {
        return TableMetadataPersistence.TableConflictHandler.valueOf(conflictHandler.name());
    }

    public static ConflictHandler hydrateFromProto(TableMetadataPersistence.TableConflictHandler conflictHandler) {
        return ConflictHandler.valueOf(conflictHandler.name());
    }
}
