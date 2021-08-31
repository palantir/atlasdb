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
package com.palantir.atlasdb.logging;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.LogSafety;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public final class SafeLoggableDataUtils {
    private static final SafeLogger log = SafeLoggerFactory.get(SafeLoggableDataUtils.class);

    private SafeLoggableDataUtils() {
        // utility
    }

    public static SafeLoggableData fromTableMetadata(Map<TableReference, byte[]> tableRefToMetadata) {
        ImmutableSafeLoggableData.Builder builder = ImmutableSafeLoggableData.builder();

        tableRefToMetadata.forEach((ref, metadataBytes) -> {
            try {
                TableMetadata tableMetadata = TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(metadataBytes);
                addLoggableNamesToBuilder(builder, ref, tableMetadata);
            } catch (Exception e) {
                log.warn("Exception thrown hydrating table metadata for table {}.", UnsafeArg.of("tableName", ref), e);
            }
        });
        return builder.build();
    }

    @VisibleForTesting
    static void addLoggableNamesToBuilder(
            ImmutableSafeLoggableData.Builder builder, TableReference ref, TableMetadata tableMetadata) {
        if (isSafe(tableMetadata.getNameLogSafety())) {
            builder.addPermittedTableReferences(ref);
        }
        // this is a system table with empty metadata, but safe for logging.
        builder.addPermittedTableReferences(AtlasDbConstants.DEFAULT_METADATA_TABLE);

        Set<String> loggableRowComponentNames = tableMetadata.getRowMetadata().getRowParts().stream()
                .filter(rowComponent -> isSafe(rowComponent.getLogSafety()))
                .map(NameComponentDescription::getComponentName)
                .collect(Collectors.toSet());
        builder.putPermittedRowComponents(ref, loggableRowComponentNames);

        Set<NamedColumnDescription> namedColumns = tableMetadata.getColumns().getNamedColumns();
        if (namedColumns != null) {
            Set<String> loggableColumnNames = namedColumns.stream()
                    .filter(columnComponent -> isSafe(columnComponent.getLogSafety()))
                    .map(NamedColumnDescription::getLongName)
                    .collect(Collectors.toSet());
            builder.putPermittedColumnNames(ref, loggableColumnNames);
        }
    }

    private static boolean isSafe(LogSafety safety) {
        return safety == LogSafety.SAFE;
    }
}
