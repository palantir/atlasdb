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

package com.palantir.atlasdb.sweep.external;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;

import org.junit.Test;

import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.schema.cleanup.ImmutableStreamStoreCleanupMetadata;
import com.palantir.atlasdb.schema.stream.StreamTableType;
import com.palantir.atlasdb.table.description.ValueType;

public class SchemalessStreamStoreDeleterTest {
    private static final Namespace NAMESPACE = Namespace.DEFAULT_NAMESPACE;
    private static final String SHORT_NAME = "s";

    private final SchemalessStreamStoreDeleter deleter = new SchemalessStreamStoreDeleter(
            NAMESPACE,
            SHORT_NAME,
            ImmutableStreamStoreCleanupMetadata.builder()
                .numHashedRowComponents(1)
                .streamIdType(ValueType.VAR_LONG)
                .build());

    @Test
    public void getTableReferencePropagatesNamespace() {
        Arrays.stream(StreamTableType.values()).forEach(type ->
                assertThat(deleter.getTableReference(type))
                        .isEqualTo(TableReference.create(NAMESPACE, type.getTableName(SHORT_NAME))));
    }
}
