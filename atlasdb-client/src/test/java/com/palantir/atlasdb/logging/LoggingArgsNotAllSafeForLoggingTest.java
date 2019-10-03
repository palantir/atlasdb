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
package com.palantir.atlasdb.logging;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.table.description.TableMetadata;

public class LoggingArgsNotAllSafeForLoggingTest {
    private static final TableReference SAFE_TABLE_REFERENCE = TableReference.createFromFullyQualifiedName("foo.safe");
    private static final TableReference UNSAFE_TABLE_REFERENCE = TableReference.createFromFullyQualifiedName("foo.bar");
    private static final byte[] SAFE_TABLE_METADATA = AtlasDbConstants.GENERIC_TABLE_METADATA;
    private static final byte[] UNSAFE_TABLE_METADATA = TableMetadata.builder()
            .nameLogSafety(TableMetadataPersistence.LogSafety.UNSAFE)
            .build()
            .persistToBytes();
    private static final Map<TableReference, byte[]> TABLE_REF_TO_METADATA = ImmutableMap.of(
            SAFE_TABLE_REFERENCE, SAFE_TABLE_METADATA,
            UNSAFE_TABLE_REFERENCE, UNSAFE_TABLE_METADATA);

    public static final boolean ALL_SAFE_FOR_LOGGING = true;
    public static final boolean NOT_ALL_SAFE_FOR_LOGGING = false;

    @Test
    public void hydrateWithInitialKeyValueServiceAllSafeForLogging() {
        assertThat(LoggingArgs.getLogArbitrator()).isEqualTo(KeyValueServiceLogArbitrator.ALL_UNSAFE);

        LoggingArgs.hydrate(TABLE_REF_TO_METADATA, NOT_ALL_SAFE_FOR_LOGGING);
        assertThat(LoggingArgs.isSafe(SAFE_TABLE_REFERENCE)).isTrue();
        assertThat(LoggingArgs.isSafe(UNSAFE_TABLE_REFERENCE)).isFalse();

        LoggingArgs.hydrate(TABLE_REF_TO_METADATA, ALL_SAFE_FOR_LOGGING);
        assertThat(LoggingArgs.isSafe(SAFE_TABLE_REFERENCE)).isTrue();
        assertThat(LoggingArgs.isSafe(UNSAFE_TABLE_REFERENCE)).isFalse();

        LoggingArgs.hydrate(ImmutableMap.of(UNSAFE_TABLE_REFERENCE, UNSAFE_TABLE_METADATA), NOT_ALL_SAFE_FOR_LOGGING);
        assertThat(LoggingArgs.isSafe(SAFE_TABLE_REFERENCE)).isFalse();
        assertThat(LoggingArgs.isSafe(UNSAFE_TABLE_REFERENCE)).isFalse();
    }
}
