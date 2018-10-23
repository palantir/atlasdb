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
package com.palantir.atlasdb.schema.metadata;

import java.util.Map;
import java.util.Optional;

import com.palantir.atlasdb.schema.SchemaMetadata;
import com.palantir.processors.AutoDelegate;

@AutoDelegate
public interface SchemaMetadataService {
    /**
     * Returns {@link SchemaMetadata} for the given schema name, provided the service knows it exists.
     *
     * @param schemaName name of schema to load metadata for
     * @return Schema metadata for the given schema name (if present)
     */
    Optional<SchemaMetadata> loadSchemaMetadata(String schemaName);

    /**
     * Stores {@link SchemaMetadata} for the given schema.
     *
     * @param schemaName name of schema to store metadata for
     * @param schemaMetadata schema metadata to be stored
     */
    void putSchemaMetadata(String schemaName, SchemaMetadata schemaMetadata);

    /**
     * Returns {@link SchemaMetadata} for all schemas known by this service.
     * This query may be costly on some implementations if many schemas are present.
     *
     * @return Map of Schema Name to Schema Metadata for the relevant schema
     */
    Map<String, SchemaMetadata> getAllSchemaMetadata();

    /**
     * Removes {@link SchemaMetadata} for the given schema name, provided it exists.
     * This method is idempotent - it will not throw even if the provided schema name does not exist in this service.
     *
     * @param schemaName name of schema to delete metadata for
     */
    void deleteSchemaMetadata(String schemaName);

    /**
     * Returns true if and only if the Schema Metadata Service has completed its initialization, and is prepared
     * to service requests.
     *
     * Note that this does not guarantee that requests can actually be serviced. For example, if the schema metadata
     * service was able to start but then the key value service became unavailable, calls to other methods will
     * still fail even though this method can return true.
     *
     * @return true iff the Schema Metadata Service has completed initialization and is prepared to service requests
     */
    boolean isInitialized();
}
