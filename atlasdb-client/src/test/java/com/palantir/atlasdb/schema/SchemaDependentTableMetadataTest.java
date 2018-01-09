/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.schema;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;

import org.junit.Test;

import com.palantir.atlasdb.protos.generated.SchemaMetadataPersistence;

public class SchemaDependentTableMetadataTest {
    @Test
    public void canSerializeAndDeserializeMetadataWithVariousCleanupRequirements() {
        Arrays.stream(SchemaMetadataPersistence.CleanupRequirement.values())
                .forEach(cleanupRequirement -> {
                    SchemaDependentTableMetadata tableMetadata = ImmutableSchemaDependentTableMetadata.builder()
                            .cleanupRequirement(cleanupRequirement)
                            .build();
                    assertThat(SchemaDependentTableMetadata.HYDRATOR.hydrateFromBytes(tableMetadata.persistToBytes()))
                            .isEqualTo(tableMetadata);
                });
    }
}
