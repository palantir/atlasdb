/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.dbkvs;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import java.util.Map;
import org.immutables.value.Value;

public interface AlterTableMetadataReference {
    boolean doesTableReferenceOrPhysicalTableNameMatch(TableReference tableReference, String physicalTableName);

    @JsonCreator
    static AlterTableMetadataReference of(Map<String, Object> jsonNode) {
        // Preserves back compat with the original config
        if (jsonNode.containsKey("physicalTableName")) {
            return of((String) jsonNode.get("physicalTableName"));
        } else if (jsonNode.get("namespace") instanceof Map && jsonNode.containsKey("tablename")) {
            Map<String, String> namespace = (Map<String, String>) jsonNode.get("namespace");
            return of(Namespace.create(namespace.get("name")), (String) jsonNode.get("tablename"));
        } else {
            throw new SafeRuntimeException(
                    "The alterTablesOrMetadataToMatchAndIKnowWhatIAmDoing value is specified incorrectly."
                            + " Please either specify a physical table name via `physicalTableName: physicalTableName`,"
                            + " or as a table reference via \n`namespace:\n  name: namespace\ntablename: tableName",
                    SafeArg.of("node", jsonNode));
        }
    }

    static AlterTableMetadataReference of(Namespace namespace, String tableName) {
        return ImmutableTableReferenceWrapper.of(TableReference.create(namespace, tableName));
    }

    static AlterTableMetadataReference of(String physicalTableName) {
        return ImmutablePhysicalTableNameWrapper.of(physicalTableName);
    }

    @Value.Immutable
    interface TableReferenceWrapper extends AlterTableMetadataReference {
        @Value.Parameter
        TableReference tableReference();

        @Override
        default boolean doesTableReferenceOrPhysicalTableNameMatch(
                TableReference tableReference, String _physicalTableName) {
            return tableReference().equals(tableReference);
        }
    }

    @Value.Immutable
    interface PhysicalTableNameWrapper extends AlterTableMetadataReference {
        @Value.Parameter
        String physicalTableName();

        @Override
        default boolean doesTableReferenceOrPhysicalTableNameMatch(
                TableReference _tableReference, String physicalTableName) {
            return physicalTableName().equals(physicalTableName);
        }
    }
}
