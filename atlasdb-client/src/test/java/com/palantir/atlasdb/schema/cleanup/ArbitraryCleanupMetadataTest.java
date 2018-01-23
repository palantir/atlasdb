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

package com.palantir.atlasdb.schema.cleanup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

import com.google.protobuf.InvalidProtocolBufferException;

public class ArbitraryCleanupMetadataTest {
    @Test
    public void distinctArbitraryCleanupMetadatasAreConsideredEqual() {
        ArbitraryCleanupMetadata metadata1 = new ArbitraryCleanupMetadata();
        ArbitraryCleanupMetadata metadata2 = new ArbitraryCleanupMetadata();

        assertThat(metadata1).isNotSameAs(metadata2)
                .isEqualTo(metadata2);
    }

    @Test
    public void canSerializeAndDeserialize() {
        ArbitraryCleanupMetadata metadata = new ArbitraryCleanupMetadata();
        assertThat(ArbitraryCleanupMetadata.BYTES_HYDRATOR.hydrateFromBytes(metadata.persistToBytes()))
                .isEqualTo(metadata);
    }

    @Test
    public void cannotDeserializeMalformedByteArray() {
        byte[] badBytes = {1};
        assertThatThrownBy(() -> ArbitraryCleanupMetadata.BYTES_HYDRATOR.hydrateFromBytes(badBytes))
                .isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(InvalidProtocolBufferException.class);
    }
}
