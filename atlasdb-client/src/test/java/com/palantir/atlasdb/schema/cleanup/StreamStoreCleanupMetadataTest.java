/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

import java.util.Collection;
import java.util.List;

import org.immutables.value.Value;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.protos.generated.SchemaMetadataPersistence;
import com.palantir.atlasdb.table.description.ValueType;

@RunWith(Enclosed.class)
public class StreamStoreCleanupMetadataTest {
    @RunWith(Parameterized.class)
    public static class HashComponentsParameterizedStreamStoreCleanupMetadataTest {
        @Parameterized.Parameters(name = "canSerializeAndDeserialize({0})")
        public static Collection<HashedComponentsAndStreamIdType> data() {
            List<Integer> validNumHashedComponents = ImmutableList.of(0, 1, 2);
            List<ValueType> valueTypes = Lists.newArrayList(ValueType.values());
            List<HashedComponentsAndStreamIdType> product = Lists.newArrayList();
            for (Integer hashedComponents : validNumHashedComponents) {
                for (ValueType valueType : valueTypes) {
                    product.add(ImmutableHashedComponentsAndStreamIdType.of(hashedComponents, valueType));
                }
            }
            return product;
        }

        private final int numComponents;
        private final ValueType streamIdType;

        public HashComponentsParameterizedStreamStoreCleanupMetadataTest(HashedComponentsAndStreamIdType testParams) {
            this.numComponents = testParams.numHashedRowComponents();
            this.streamIdType = testParams.streamIdType();
        }

        @Test
        public void canSerializeAndDeserializePreservingHashedComponentsAndValueTypes() {
            StreamStoreCleanupMetadata metadata = ImmutableStreamStoreCleanupMetadata.builder()
                    .numHashedRowComponents(numComponents)
                    .streamIdType(streamIdType)
                    .build();
            assertThat(StreamStoreCleanupMetadata.BYTES_HYDRATOR.hydrateFromBytes(metadata.persistToBytes()))
                    .isEqualTo(metadata);
        }
    }

    public static class NonParameterizedStreamStoreCleanupMetadataTest {
        @Test
        public void throwsIfDeserializingEmptyStreamStoreCleanupMetadata() {
            SchemaMetadataPersistence.StreamStoreCleanupMetadata metadataProto =
                    SchemaMetadataPersistence.StreamStoreCleanupMetadata.newBuilder().build();

            assertThatThrownBy(() -> StreamStoreCleanupMetadata.hydrateFromProto(metadataProto))
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    @Value.Immutable
    interface HashedComponentsAndStreamIdType {
        @Value.Parameter
        int numHashedRowComponents();
        @Value.Parameter
        ValueType streamIdType();
    }
}
