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

package com.palantir.atlasdb.cell.api;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import java.util.Map;
import java.util.Set;

public interface DdlManager {

    // TODO(jakubk): Convert all these to be async.
    // TODO(jakubk): Switch to using TableMetadata.
    void createTables(Map<TableReference, byte[]> tableRefToTableMetadata);

    void dropTables(Set<TableReference> tableRefs);
}
