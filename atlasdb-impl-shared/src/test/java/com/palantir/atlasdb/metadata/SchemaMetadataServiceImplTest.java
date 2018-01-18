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

package com.palantir.atlasdb.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.awaitility.Awaitility;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.ForwardingKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.schema.ImmutableSchemaDependentTableMetadata;
import com.palantir.atlasdb.schema.ImmutableSchemaMetadata;
import com.palantir.atlasdb.schema.SchemaMetadata;
import com.palantir.atlasdb.schema.cleanup.ImmutableStreamStoreCleanupMetadata;
import com.palantir.atlasdb.schema.cleanup.NullCleanupMetadata;
import com.palantir.atlasdb.schema.metadata.SchemaMetadataService;
import com.palantir.atlasdb.schema.metadata.SchemaMetadataServiceImpl;
import com.palantir.atlasdb.table.description.ValueType;

public class SchemaMetadataServiceImplTest {
    private final SchemaMetadataService SCHEMA_METADATA_SERVICE = SchemaMetadataServiceImpl.create(
            new InMemoryKeyValueService(true),
            false);

    private final String SCHEMA_NAME_ONE = "one";
    private final SchemaMetadata SCHEMA_METADATA_ONE =
            ImmutableSchemaMetadata.builder().putSchemaDependentTableMetadata(
                    TableReference.create(Namespace.EMPTY_NAMESPACE, "tableOne"),
                    ImmutableSchemaDependentTableMetadata.builder().cleanupMetadata(new NullCleanupMetadata()).build()).build();

    private final String SCHEMA_NAME_TWO = "two";
    private final SchemaMetadata SCHEMA_METADATA_TWO =
            ImmutableSchemaMetadata.builder().putSchemaDependentTableMetadata(
                    TableReference.create(Namespace.EMPTY_NAMESPACE, "tableTwo"),
                    ImmutableSchemaDependentTableMetadata.builder().cleanupMetadata(
                            ImmutableStreamStoreCleanupMetadata.builder()
                                    .numHashedRowComponents(1)
                                    .streamIdType(ValueType.VAR_LONG)
                                    .build()).build()).build();

    @Test
    public void retrievesStoredMetadata() {
        SCHEMA_METADATA_SERVICE.putSchemaMetadata(SCHEMA_NAME_ONE, SCHEMA_METADATA_ONE);
        assertThat(SCHEMA_METADATA_SERVICE.loadSchemaMetadata(SCHEMA_NAME_ONE)).contains(SCHEMA_METADATA_ONE);
    }

    @Test
    public void returnsOptionalIfNoMetadataPresent() {
        assertThat(SCHEMA_METADATA_SERVICE.loadSchemaMetadata("should not exist")).isEmpty();
    }

    @Test
    public void overwritesPreviouslyStoredMetadata() {
        SCHEMA_METADATA_SERVICE.putSchemaMetadata(SCHEMA_NAME_ONE, SCHEMA_METADATA_ONE);
        SCHEMA_METADATA_SERVICE.putSchemaMetadata(SCHEMA_NAME_ONE, SCHEMA_METADATA_TWO);
        assertThat(SCHEMA_METADATA_SERVICE.loadSchemaMetadata(SCHEMA_NAME_ONE)).contains(SCHEMA_METADATA_TWO);
    }

    @Test
    public void storesDistinctMetadataForDifferentSchemas() {
        SCHEMA_METADATA_SERVICE.putSchemaMetadata(SCHEMA_NAME_ONE, SCHEMA_METADATA_ONE);
        SCHEMA_METADATA_SERVICE.putSchemaMetadata(SCHEMA_NAME_TWO, SCHEMA_METADATA_TWO);
        assertThat(SCHEMA_METADATA_SERVICE.loadSchemaMetadata(SCHEMA_NAME_ONE)).contains(SCHEMA_METADATA_ONE);
        assertThat(SCHEMA_METADATA_SERVICE.loadSchemaMetadata(SCHEMA_NAME_TWO)).contains(SCHEMA_METADATA_TWO);
    }

    @Test
    public void getAllMetadataReturnsKnownPairs() {
        SCHEMA_METADATA_SERVICE.putSchemaMetadata(SCHEMA_NAME_ONE, SCHEMA_METADATA_ONE);
        SCHEMA_METADATA_SERVICE.putSchemaMetadata(SCHEMA_NAME_TWO, SCHEMA_METADATA_TWO);

        Map<String, SchemaMetadata> expected = ImmutableMap.of(
                SCHEMA_NAME_ONE, SCHEMA_METADATA_ONE, SCHEMA_NAME_TWO, SCHEMA_METADATA_TWO);
        assertThat(SCHEMA_METADATA_SERVICE.getAllSchemaMetadata()).isEqualTo(expected);
    }

    @Test
    public void getAllMetadataReturnsNothingIfNoDataKnown() {
        assertThat(SCHEMA_METADATA_SERVICE.getAllSchemaMetadata()).isEmpty();
    }

    @Test
    public void canDecommissionSchema() {
        SCHEMA_METADATA_SERVICE.putSchemaMetadata(SCHEMA_NAME_ONE, SCHEMA_METADATA_ONE);
        SCHEMA_METADATA_SERVICE.decommissionSchema(SCHEMA_NAME_ONE);
        assertThat(SCHEMA_METADATA_SERVICE.loadSchemaMetadata(SCHEMA_NAME_ONE)).isEmpty();
    }

    @Test
    public void canDecommissionAndRecommissionSchema() {
        IntStream.range(0, 10)
                .forEach(index -> {
                    SchemaMetadata metadataToPut = index % 2 == 0 ? SCHEMA_METADATA_ONE : SCHEMA_METADATA_TWO;
                    SCHEMA_METADATA_SERVICE.putSchemaMetadata(SCHEMA_NAME_ONE, metadataToPut);
                    assertThat(SCHEMA_METADATA_SERVICE.loadSchemaMetadata(SCHEMA_NAME_ONE))
                            .contains(metadataToPut);
                    SCHEMA_METADATA_SERVICE.decommissionSchema(SCHEMA_NAME_ONE);
                    assertThat(SCHEMA_METADATA_SERVICE.loadSchemaMetadata(SCHEMA_NAME_ONE)).isEmpty();
                });
    }

    @Test
    public void canDecommissionSchemaThatIsNotPresent() {
        SCHEMA_METADATA_SERVICE.decommissionSchema("should not exist");
        // pass
    }

    @Test
    public void decommissionIsIdempotent() {
        SCHEMA_METADATA_SERVICE.putSchemaMetadata(SCHEMA_NAME_ONE, SCHEMA_METADATA_ONE);
        SCHEMA_METADATA_SERVICE.decommissionSchema(SCHEMA_NAME_ONE);
        SCHEMA_METADATA_SERVICE.decommissionSchema(SCHEMA_NAME_ONE);
        assertThat(SCHEMA_METADATA_SERVICE.loadSchemaMetadata(SCHEMA_NAME_ONE)).isEmpty();
    }

    @Test
    public void canInitializeAsynchronously() {
        ForwardingKeyValueService forwardingKeyValueService = new ForwardingKeyValueService() {
            private KeyValueService realKeyValueService = new InMemoryKeyValueService(true);
            private boolean fail = true;

            @Override
            protected KeyValueService delegate() {
                if (fail) {
                    fail = false;
                    return mock(KeyValueService.class, (Answer) invocation -> {
                        throw new RuntimeException("I am unhappy");
                    });
                }
                return realKeyValueService;
            }
        };

        SchemaMetadataService schemaMetadataService = SchemaMetadataServiceImpl.create(
                forwardingKeyValueService,
                true);

        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(schemaMetadataService::isInitialized);
        schemaMetadataService.putSchemaMetadata(SCHEMA_NAME_ONE, SCHEMA_METADATA_ONE);
        assertThat(schemaMetadataService.loadSchemaMetadata(SCHEMA_NAME_ONE)).contains(SCHEMA_METADATA_ONE);
    }
}
