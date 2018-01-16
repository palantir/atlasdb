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

import java.util.stream.IntStream;

import org.junit.Test;

import com.palantir.atlasdb.table.description.ValueType;

public class CleanupMetadataTest {
    @Test
    public void visitorThrowsIfMethodUnimplemented() {
        assertThatThrownBy(() -> new HashComponentsVisitor().visit(new ArbitraryCleanupMetadata()))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> new HashComponentsVisitor().visit(new NullCleanupMetadata()))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void canInvokeMethodsOnVisitor() {
        IntStream.range(0, 2)
                .forEach(num ->
                        assertThat(new HashComponentsVisitor().visit(
                                ImmutableStreamStoreCleanupMetadata.builder()
                                        .numHashedRowComponents(num)
                                        .streamIdType(ValueType.UUID)
                                        .build())).isEqualTo(num));
    }

    private static class HashComponentsVisitor implements CleanupMetadata.Visitor<Integer> {
        @Override
        public Integer visit(StreamStoreCleanupMetadata metadata) {
            return metadata.numHashedRowComponents();
        }
    }
}
