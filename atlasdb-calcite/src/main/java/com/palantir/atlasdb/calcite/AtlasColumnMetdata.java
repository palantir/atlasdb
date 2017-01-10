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

import java.util.Optional;
import java.util.UUID;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.immutables.value.Value;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.util.crypto.Sha256Hash;

/**
 * Represents a JDBC column, which corresponds to an AtlasDB row component, named column, or dynamic column.
 */
@Value.Immutable
abstract class AtlasColumnMetdata {
    public abstract TableMetadata metadata();
    public abstract Optional<NameComponentDescription> component();
    public abstract Optional<NamedColumnDescription> column();
    public abstract Optional<ColumnValueDescription> value();

    @Value.Derived
    public boolean isComponent() {
        return component().isPresent();
    }

    @Value.Derived
    public boolean isNamedColumn() {
        return column().isPresent();
    }

    @Value.Derived
    public boolean isValue() {
        return value().isPresent();
    }

    @Value.Default
    public boolean dynamicColumn() {
        return false;
    }

    @Value.Derived
    public ValueType valueType() {
        if (isComponent()) {
            return component().get().getType();
        } else if (isNamedColumn()) {
            return column().get().getValue().getValueType();
        } else if (isValue()) {
            return value().get().getValueType();
        }
        throw new IllegalStateException();
    }

    @Value.Derived
    public TableMetadataPersistence.ValueByteOrder byteOrder() {
        if (isComponent()) {
            return component().get().getOrder();
        } else if (isNamedColumn() || isValue()) {
            return TableMetadataPersistence.ValueByteOrder.ASCENDING;
        }
        throw new IllegalStateException();
    }

    @Value.Derived
    public ColumnValueDescription.Format format() {
        if (isComponent()) {
            return ColumnValueDescription.Format.VALUE_TYPE;
        } else if (isNamedColumn()) {
            return column().get().getValue().getFormat();
        } else if (isValue()) {
            return value().get().getFormat();
        }
        throw new IllegalStateException();
    }

    @Value.Derived
    public String getDisplayName() {
        if (isComponent()) {
            return component().get().getComponentName();
        } else if (isNamedColumn()) {
            return column().get().getLongName();
        } else if (isValue()) {
            return "val";
        }
        throw new IllegalStateException();
    }

    @Value.Derived
    public String getName() {
        if (column().isPresent()) {
            return column().get().getShortName();
        }
        return getDisplayName();
    }

    public RelDataType relDataType(RelDataTypeFactory factory) {
        RelDataType type = valueTypeToDataType(factory);
        if (isComponent() || type.isNullable()) {
            return type;
        }
        return factory.createTypeWithNullability(type, true);
    }

    private RelDataType valueTypeToDataType(RelDataTypeFactory factory) {
        switch (format()) {
            case PROTO:
                return factory.createMapType(
                        factory.createJavaType(String.class),
                        factory.createSqlType(SqlTypeName.ANY));
            case PERSISTABLE:
            case PERSISTER:
                return factory.createSqlType(SqlTypeName.BINARY);
            case VALUE_TYPE:
                switch (valueType()) {
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
                        throw new IllegalStateException("Unknown ValueType: " + valueType());
                }
            default:
                throw new IllegalStateException("Unknown Format: " + format());
        }
    }

    public Object deserialize(byte[] bytes) {
        if (isComponent()) {
            throw new RuntimeException("Row/column components cannot be deserialized in isolation."
                    + "Use EncodingUtils.fromBytes() instead.");
        }
        switch (format()) {
            case PROTO:
                if (isComponent()) {
                    throw new IllegalStateException("Row/column components cannot contain protobufs (invalid metadata).");
                } else if (isNamedColumn()) {
                    return ProtobufDeserializers.convertMessageToMap(
                            column().get().getValue().hydrateProto(
                                    Thread.currentThread().getContextClassLoader(), bytes));
                } else if (isValue()) {
                    return ProtobufDeserializers.convertMessageToMap(
                            value().get().hydrateProto(
                                    Thread.currentThread().getContextClassLoader(), bytes));
                }
            case PERSISTABLE:
            case PERSISTER:
                return bytes;
            case VALUE_TYPE:
                return Iterables.getOnlyElement(
                        EncodingUtils.fromBytes(bytes,
                                ImmutableList.of(new EncodingUtils.EncodingType(valueType(), byteOrder()))));
            default:
                throw new IllegalStateException("Unknown Format: " + format());
        }
    }

    @Value.Check
    public void check() {
        Preconditions.checkArgument(component().isPresent() ^ column().isPresent() ^ value().isPresent(),
                "Exactly one of either a component, column, or value must be given!");
    }
}
