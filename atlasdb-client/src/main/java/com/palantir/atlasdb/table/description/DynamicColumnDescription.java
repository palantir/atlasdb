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
package com.palantir.atlasdb.table.description;

import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import javax.annotation.concurrent.Immutable;

@Immutable
public class DynamicColumnDescription {
    final NameMetadataDescription columnNameDesc;
    final ColumnValueDescription value;

    public DynamicColumnDescription(NameMetadataDescription desc, ColumnValueDescription value) {
        this.columnNameDesc = desc;
        this.value = value;
    }

    public NameMetadataDescription getColumnNameDesc() {
        return columnNameDesc;
    }

    public ColumnValueDescription getValue() {
        return value;
    }

    public TableMetadataPersistence.DynamicColumnDescription.Builder persistToProto() {
        TableMetadataPersistence.DynamicColumnDescription.Builder builder =
                TableMetadataPersistence.DynamicColumnDescription.newBuilder();
        builder.setColumnNameDesc(columnNameDesc.persistToProto());
        builder.setValue(value.persistToProto());
        return builder;
    }

    public static DynamicColumnDescription hydrateFromProto(TableMetadataPersistence.DynamicColumnDescription message) {
        return new DynamicColumnDescription(
                NameMetadataDescription.hydrateFromProto(message.getColumnNameDesc()),
                ColumnValueDescription.hydrateFromProto(message.getValue()));
    }

    @Override
    public String toString() {
        return "DynamicColumnDescription [columnNameDesc=" + columnNameDesc + ", value=" + value + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;

        int result = 1;
        result = prime * result + (columnNameDesc == null ? 0 : columnNameDesc.hashCode());
        result = prime * result + (value == null ? 0 : value.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        DynamicColumnDescription other = (DynamicColumnDescription) obj;
        if (columnNameDesc == null) {
            if (other.getColumnNameDesc() != null) {
                return false;
            }
        } else if (!columnNameDesc.equals(other.getColumnNameDesc())) {
            return false;
        }
        if (value == null) {
            if (other.getValue() != null) {
                return false;
            }
        } else if (!value.equals(other.getValue())) {
            return false;
        }
        return true;
    }
}
