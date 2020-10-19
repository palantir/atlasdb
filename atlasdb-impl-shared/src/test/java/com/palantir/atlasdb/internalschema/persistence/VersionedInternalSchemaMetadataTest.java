/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import org.junit.Test;

public class VersionedInternalSchemaMetadataTest {
    private static final Path BASE_METADATA_PATH = getResourcePath("metadata-v1-base.json");
    private static final byte[] BASE_METADATA_BYTES = readAllBytesUnchecked(BASE_METADATA_PATH);
    private static final VersionedInternalSchemaMetadata V1_BASE = createVersionedMetadata(1, BASE_METADATA_BYTES);

    @Test
    public void metadataSemanticallyEquivalentToItselfIfCanBeDeserializedAndSemanticsAgree() {
        assertThat(V1_BASE.underlyingData()).isPresent();
        assertThat(V1_BASE.knowablySemanticallyEquivalent(V1_BASE)).isTrue();
    }

    @Test
    public void metadataNotSemanticallyEquivalentIfSemanticsAreDifferent() {
        byte[] otherMetadata = readAllBytesUnchecked(getResourcePath("metadata-v1-different.json"));
        VersionedInternalSchemaMetadata versionedMetadata = createVersionedMetadata(1, otherMetadata);
        assertThat(V1_BASE.knowablySemanticallyEquivalent(versionedMetadata)).isFalse();
    }

    @Test
    public void metadataNotSemanticallyEquivalentToItselfIfCannotDeserialize() {
        VersionedInternalSchemaMetadata v2Base = createVersionedMetadata(2, BASE_METADATA_BYTES);
        assertThat(v2Base.underlyingData()).isNotPresent();
        assertThat(v2Base.knowablySemanticallyEquivalent(v2Base)).isFalse();
    }

    @Test
    public void metadataSemanticallyEquivalentEvenIfKeySerializationOrderDifferent() {
        byte[] differentKeyOrderMetadata = readAllBytesUnchecked(getResourcePath("metadata-v1-diff-key-order.json"));
        VersionedInternalSchemaMetadata v1DifferentKeyOrder = createVersionedMetadata(1, differentKeyOrderMetadata);
        assertThat(V1_BASE).isNotEqualTo(v1DifferentKeyOrder);
        assertThat(V1_BASE.knowablySemanticallyEquivalent(v1DifferentKeyOrder)).isTrue();
    }

    @Test
    public void metadataSemanticallyEquivalentEvenIfMapSerializationOrderDifferent() {
        byte[] differentMapOrderMetadata = readAllBytesUnchecked(getResourcePath("metadata-v1-diff-map-order.json"));
        VersionedInternalSchemaMetadata v1DifferentMapOrder = createVersionedMetadata(1, differentMapOrderMetadata);
        assertThat(V1_BASE).isNotEqualTo(v1DifferentMapOrder);
        assertThat(V1_BASE.knowablySemanticallyEquivalent(v1DifferentMapOrder)).isTrue();
    }

    private static VersionedInternalSchemaMetadata createVersionedMetadata(int version, byte[] payload) {
        return ImmutableVersionedInternalSchemaMetadata.builder()
                .version(version)
                .payload(payload)
                .build();
    }

    private static Path getResourcePath(String subPath) {
        try {
            return Paths.get(Objects.requireNonNull(
                    InternalSchemaMetadataPayloadCodecTest.class.getClassLoader().getResource(
                            "internalschema-persistence/" + subPath)).toURI());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private static byte[] readAllBytesUnchecked(Path baseMetadataPath) {
        try {
            return Files.readAllBytes(baseMetadataPath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
