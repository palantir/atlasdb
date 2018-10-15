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
package com.palantir.atlasdb.schema.cleanup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

import com.google.protobuf.InvalidProtocolBufferException;

public class NullCleanupMetadataTest {
    @Test
    public void distinctNullCleanupMetadatasAreConsideredEqual() {
        NullCleanupMetadata metadata1 = new NullCleanupMetadata();
        NullCleanupMetadata metadata2 = new NullCleanupMetadata();

        assertThat(metadata1).isNotSameAs(metadata2)
                .isEqualTo(metadata2);
    }

    @Test
    public void canSerializeAndDeserialize() {
        NullCleanupMetadata metadata = new NullCleanupMetadata();
        assertThat(NullCleanupMetadata.BYTES_HYDRATOR.hydrateFromBytes(metadata.persistToBytes()))
                .isEqualTo(metadata);
    }

    @Test
    public void cannotDeserializeMalformedByteArray() {
        byte[] badBytes = {1};
        assertThatThrownBy(() -> NullCleanupMetadata.BYTES_HYDRATOR.hydrateFromBytes(badBytes))
                .isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(InvalidProtocolBufferException.class);
    }
}
