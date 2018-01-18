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

package com.palantir.atlasdb.ete;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import com.palantir.atlasdb.schema.CleanupMetadataResource;
import com.palantir.atlasdb.schema.SerializableCleanupMetadata;
import com.palantir.atlasdb.todo.TodoSchema;

public class SchemaMetadataServiceEteTest {
    @Test
    public void shouldBeAbleToRetrieveSchemaMetadataForTodoTable() {
        CleanupMetadataResource resource = EteSetup.createClientToSingleNode(CleanupMetadataResource.class);

        assertThat(resource.get(
                TodoSchema.class.getSimpleName(),
                "default.todo")).isPresent()
                .hasValueSatisfying(serializableCleanupMetadata ->
                        assertThat(serializableCleanupMetadata.cleanupMetadataType()).isEqualTo(
                                SerializableCleanupMetadata.NULL_TYPE));
    }

}
