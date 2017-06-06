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
import java.util.stream.Collectors;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.TableMetadata;

public final class SafeLoggableDataUtils {
    private SafeLoggableDataUtils() {
        // utility
    }

    public static SafeLoggableData fromTableMetadata(Map<TableReference, byte[]> tableMetadataMap) {
        ImmutableSafeLoggableData.Builder builder = ImmutableSafeLoggableData.builder();

        tableMetadataMap.forEach(
                (ref, metadataBytes) -> {
                    TableMetadata tableMetadata = TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(metadataBytes);
                    addLoggableNamesToBuilder(builder, ref, tableMetadata);
                });
        return builder.build();
    }

    private static void addLoggableNamesToBuilder(
            ImmutableSafeLoggableData.Builder builder,
            TableReference ref,
            TableMetadata tableMetadata) {
        if (tableMetadata.isNameLoggable()) {
            builder.addPermittedTableReferences(ref);
        }

        Set<NameComponentDescription> loggableRowParts = tableMetadata.getRowMetadata()
                .getRowParts()
                .stream()
                .filter(NameComponentDescription::isNameLoggable)
                .collect(Collectors.toSet());
        builder.putPermittedRowComponents(ref, loggableRowParts);

        Set<NamedColumnDescription> namedColumns = tableMetadata.getColumns().getNamedColumns();
        if (namedColumns != null) {
            Set<NamedColumnDescription> loggableColumnDescriptions = namedColumns
                    .stream()
                    .filter(NamedColumnDescription::isNameLoggable)
                    .collect(Collectors.toSet());
            builder.putPermittedColumnNames(ref, loggableColumnDescriptions);
        }
    }
}
