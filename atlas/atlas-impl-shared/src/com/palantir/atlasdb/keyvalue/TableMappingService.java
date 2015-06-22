// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.keyvalue;

import java.util.Map;
import java.util.Set;

import com.palantir.atlasdb.schema.Namespace;
import com.palantir.atlasdb.schema.TableReference;


public interface TableMappingService {
    public String addTable(TableReference tableRef);
    public void removeTable(TableReference tableRef);
    public void updateTableMap();
    public String getShortTableName(TableReference tableRef);
    public <T> Map<String, T> mapToShortTableNames(Map<TableReference, T> tableMap);
    public Set<TableReference> mapToFullTableNames(Set<String> tableNames);
    public boolean isInitializedNamespace(Namespace namespace);
}
