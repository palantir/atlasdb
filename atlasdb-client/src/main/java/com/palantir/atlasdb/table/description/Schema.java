/**
 * Copyright 2015 Palantir Technologies
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

import java.util.Map;
import java.util.Set;

import com.google.common.collect.Multimap;
import com.palantir.atlasdb.cleaner.api.OnCleanupTask;
import com.palantir.atlasdb.schema.Namespace;

public interface Schema {

	TableDefinition getTableDefinition(String tableName);

	Map<String, TableMetadata> getAllTablesAndIndexMetadata();

	Set<String> getAllIndexes();

	IndexDefinition getIndex(String indexName);

	IndexDefinition getIndexForShortName(String indexName);

	/**
	 * Performs some basic checks on this schema to check its validity.
	 */
	void validate();

	Map<String, TableDefinition> getTableDefinitions();

	Map<String, IndexDefinition> getIndexDefinitions();

	Namespace getNamespace();

	Multimap<String, OnCleanupTask> getCleanupTasksByTable();

}