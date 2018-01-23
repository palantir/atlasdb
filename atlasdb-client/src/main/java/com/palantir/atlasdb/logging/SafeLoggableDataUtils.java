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

package com.palantir.atlasdb.logging;

import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.LogSafety;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.TableMetadata;

public final class SafeLoggableDataUtils {
    private static final Predicate<LogSafety> IS_SAFE = safety -> safety == LogSafety.SAFE;

    private SafeLoggableDataUtils() {
        // utility
    }

    public static SafeLoggableData fromTableMetadata(Map<TableReference, byte[]> tableRefToMetadata) {
        ImmutableSafeLoggableData.Builder builder = ImmutableSafeLoggableData.builder();

        tableRefToMetadata.forEach(
                (ref, metadataBytes) -> {
                    TableMetadata tableMetadata = TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(metadataBytes);
                    addLoggableNamesToBuilder(builder, ref, tableMetadata);
                });
        return builder.build();
    }

    @VisibleForTesting
    static void addLoggableNamesToBuilder(
            ImmutableSafeLoggableData.Builder builder,
            TableReference ref,
            TableMetadata tableMetadata) {
        if (IS_SAFE.test(tableMetadata.getNameLogSafety())) {
            builder.addPermittedTableReferences(ref);
        }
        // this is a system table with empty metadata, but safe for logging.
        builder.addPermittedTableReferences(AtlasDbConstants.DEFAULT_METADATA_TABLE);

        Set<String> loggableRowComponentNames = tableMetadata.getRowMetadata()
                .getRowParts()
                .stream()
                .filter(rowComponent -> IS_SAFE.test(rowComponent.getLogSafety()))
                .map(NameComponentDescription::getComponentName)
                .collect(Collectors.toSet());
        builder.putPermittedRowComponents(ref, loggableRowComponentNames);

        Set<NamedColumnDescription> namedColumns = tableMetadata.getColumns().getNamedColumns();
        if (namedColumns != null) {
            Set<String> loggableColumnNames = namedColumns
                    .stream()
                    .filter(columnComponent -> IS_SAFE.test(columnComponent.getLogSafety()))
                    .map(NamedColumnDescription::getLongName)
                    .collect(Collectors.toSet());
            builder.putPermittedColumnNames(ref, loggableColumnNames);
        }
    }
}
