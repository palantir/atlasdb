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

package com.palantir.atlasdb.internalschema.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.Range;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadata;
import com.palantir.atlasdb.internalschema.TimestampPartitioningMap;
import com.palantir.conjure.java.serialization.ObjectMappers;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;
import org.junit.Test;

public class InternalSchemaMetadataPayloadCodecTest {
    private static final InternalSchemaMetadata INTERNAL_SCHEMA_METADATA = InternalSchemaMetadata.builder()
            .timestampToTransactionsTableSchemaVersion(
                    TimestampPartitioningMap.of(ImmutableRangeMap.<Long, Integer>builder()
                            .put(Range.closedOpen(1L, 1000L), 1)
                            .put(Range.atLeast(1000L), 2)
                            .build()))
            .build();

    @Test
    public void cannotDeserializeUnknownVersionOfMetadata() {
        assertThatThrownBy(() ->
                        InternalSchemaMetadataPayloadCodec.decode(ImmutableVersionedInternalSchemaMetadata.builder()
                                .version(8377466)
                                .payload(new byte[] {1, 2, 3})
                                .build()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Could not decode persisted internal schema metadata - unrecognized version.");
    }

    @Test
    public void serializesToTheLatestVersion() {
        VersionedInternalSchemaMetadata encoded = InternalSchemaMetadataPayloadCodec.encode(INTERNAL_SCHEMA_METADATA);
        assertThat(encoded.version()).isEqualTo(InternalSchemaMetadataPayloadCodec.LATEST_VERSION);
    }

    @Test
    public void canSerializeAndDeserialize() {
        VersionedInternalSchemaMetadata encoded = InternalSchemaMetadataPayloadCodec.encode(INTERNAL_SCHEMA_METADATA);
        assertThat(InternalSchemaMetadataPayloadCodec.decode(encoded)).isEqualTo(INTERNAL_SCHEMA_METADATA);
    }

    @Test
    public void canDeserializeV1Metadata() throws URISyntaxException, IOException {
        String resourcePath = getResourcePath("internalschema-persistence/versioned-metadata-v1-1.json");
        byte[] bytes = Files.readAllBytes(Paths.get(resourcePath));
        VersionedInternalSchemaMetadata versionedInternalSchemaMetadata =
                ObjectMappers.newServerObjectMapper().readValue(bytes, VersionedInternalSchemaMetadata.class);
        assertThat(InternalSchemaMetadataPayloadCodec.decode(versionedInternalSchemaMetadata))
                .isEqualTo(INTERNAL_SCHEMA_METADATA);
    }

    private static String getResourcePath(String subPath) throws URISyntaxException {
        return Paths.get(Objects.requireNonNull(InternalSchemaMetadataPayloadCodecTest.class
                                .getClassLoader()
                                .getResource(subPath))
                        .toURI())
                .toString();
    }
}
