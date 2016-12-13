/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.calcite;

import java.util.NoSuchElementException;
import java.util.UUID;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.DynamicColumnDescription;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.util.crypto.Sha256Hash;

public class AtlasEnumerator implements Enumerator<Object[]> {
    private boolean ready = false;

    @Override
    public Object[] current() {
        if (ready) {
            return new String[] { "entry1" };
        }
        throw new NoSuchElementException();
    }

    @Override
    public boolean moveNext() {
        boolean ret = ready;
        ready = true;
        return !ret;
    }

    @Override
    public void reset() {
        ready = false;
    }

    @Override
    public void close() {
        // nothing to close
    }

    public static RelDataType deduceRowType(RelDataTypeFactory factory, TableMetadata metadata) {
        ImmutableMap.Builder<String, RelDataType> builder = ImmutableMap.builder();

        for (NameComponentDescription meta : metadata.getRowMetadata().getRowParts()) {
            builder.put(meta.getComponentName(),
                    toRelDataType(factory, meta.getType()));
        }
        for (NamedColumnDescription meta : metadata.getColumns().getNamedColumns()) {
            String colName = meta.getLongName();
            ColumnValueDescription value = meta.getValue();
            switch (value.getFormat()) {
                case VALUE_TYPE:
                    builder.put(colName, toRelDataType(factory, value.getValueType()));
                    break;
                case PROTO:
                case PERSISTABLE:
                case PERSISTER:
                    throw new UnsupportedOperationException("Cannot decode protobufs, persitables, or persisters yet!");
            }
        }
        DynamicColumnDescription dynCol = metadata.getColumns().getDynamicColumn();
        if (dynCol != null) {
            throw new UnsupportedOperationException("Cannot decode dynamic columns yet!");
        }

        return factory.createStructType(Lists.newArrayList(builder.build().entrySet()));
    }

    private static RelDataType toRelDataType(RelDataTypeFactory factory, ValueType type) {
        switch (type) {
            case VAR_LONG:
            case VAR_SIGNED_LONG:
            case FIXED_LONG:
            case FIXED_LONG_LITTLE_ENDIAN:
                return factory.createJavaType(Long.class);
            case NULLABLE_FIXED_LONG:
                return factory.createTypeWithNullability(factory.createJavaType(Long.class), true);
            case VAR_STRING:
            case STRING:
                // for now we choose to represent ValueType blobs as strings
            case BLOB:
            case SIZED_BLOB:
                return factory.createJavaType(String.class);
            case SHA256HASH:
                return factory.createJavaType(Sha256Hash.class);
            case UUID:
                return factory.createJavaType(UUID.class);
            default:
                throw new IllegalStateException("Unknown ValueType: " + type);
        }
    }
}
