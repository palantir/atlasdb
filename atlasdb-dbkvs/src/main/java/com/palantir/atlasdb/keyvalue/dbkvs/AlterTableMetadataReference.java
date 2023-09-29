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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import org.immutables.value.Value;

public interface AlterTableMetadataReference {
    boolean doesTableReferenceOrPhysicalTableNameMatch(TableReference tableReference, String physicalTableName);

    @JsonCreator
    static AlterTableMetadataReference of(
            @JsonProperty("namespace") Namespace namespace, @JsonProperty("tablename") String tableName) {
        return ImmutableTableReferenceWrapper.of(TableReference.create(namespace, tableName));
    }

    @JsonCreator
    static AlterTableMetadataReference of(@JsonProperty("physicalTableName") String physicalTableName) {
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
