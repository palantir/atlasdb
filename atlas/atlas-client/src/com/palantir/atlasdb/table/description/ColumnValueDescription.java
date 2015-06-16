// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.table.description;

import java.lang.reflect.Method;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.Message;
import com.googlecode.protobuf.format.JsonFormat;
import com.googlecode.protobuf.format.JsonFormat.ParseException;
import com.palantir.atlasdb.compress.CompressionUtils;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.ColumnValueDescription.Builder;
import com.palantir.atlasdb.table.generation.ColumnValues;
import com.palantir.common.base.Throwables;
import com.palantir.common.persist.JsonPersistable;
import com.palantir.common.persist.JsonPersistable.Hydrator;
import com.palantir.common.persist.JsonPersistables;
import com.palantir.common.persist.Persistable;
import com.palantir.common.persist.Persistables;

@Immutable
public class ColumnValueDescription {
    private static final Logger log = LoggerFactory.getLogger(ColumnValueDescription.class);

    public enum Format {
        PROTO,
        PERSISTABLE,
        VALUE_TYPE,
        BLOCK_STORED_PROTO;

        public TableMetadataPersistence.ColumnValueFormat persistToProto() {
            return TableMetadataPersistence.ColumnValueFormat.valueOf(name());
        }

        public static Format hydrateFromProto(TableMetadataPersistence.ColumnValueFormat message) {
            return valueOf(message.name());
        }
    }

    public enum Compression {
        SNAPPY,
        NONE;

        public TableMetadataPersistence.Compression persistToProto() {
            return TableMetadataPersistence.Compression.valueOf(name());
        }

        public static Compression hydrateFromProto(TableMetadataPersistence.Compression compression) {
            return valueOf(compression.name());
        }
    }

    final Format format;
    final Compression compression;
    final ValueType type;
    @Nullable final String className; // null if format is VALUE_TYPE
    @Nullable final String canonicalClassName; // null if format is VALUE_TYPE
    // null if not a proto or descriptor is missing
    @Nullable final Descriptor protoDescriptor;

    private ColumnValueDescription(ValueType type, Compression compression) {
        this.format = Format.VALUE_TYPE;
        this.compression = Preconditions.checkNotNull(compression);
        this.type = Preconditions.checkNotNull(type);
        this.canonicalClassName = null;
        this.className = null;
        this.protoDescriptor = null;
    }

    public static ColumnValueDescription forType(ValueType type) {
        return forType(type, Compression.NONE);
    }

    public static ColumnValueDescription forType(ValueType type,
                                                 Compression compression) {
        return new ColumnValueDescription(type, compression);
    }

    public static ColumnValueDescription forPersistable(Class<? extends Persistable> clazz) {
        return forPersistable(clazz, Compression.NONE);
    }

    @SuppressWarnings("unchecked")
    public static ColumnValueDescription forPersistable(Class<? extends Persistable> clazz,
                                                        Compression compression) {
        Validate.notNull(Persistables.getHydrator(clazz), "Not a valid persistable class because it has no hydrator");
        if (JsonPersistable.class.isAssignableFrom(clazz)) {
            Validate.notNull(JsonPersistables.getHydrator((Class<? extends JsonPersistable>)clazz), "Not a valid jsonPersistable class because it has no hydrator");
        }
        return new ColumnValueDescription(Format.PERSISTABLE, clazz.getName(), clazz.getCanonicalName(), compression, null);
    }

    public static ColumnValueDescription forProtoMessage(Class<? extends GeneratedMessage> clazz) {
        return forProtoMessage(clazz, Compression.NONE);
    }

    public static ColumnValueDescription forProtoMessage(Class<? extends GeneratedMessage> clazz,
                                                         Compression compression) {
        return new ColumnValueDescription(
                Format.PROTO,
                clazz.getName(),
                clazz.getCanonicalName(),
                compression,
                getDescriptor(clazz));
    }

    private static <T extends GeneratedMessage> Descriptor getDescriptor(Class<T> clazz) {
        try {
            Method method = clazz.getMethod("getDescriptor");
            return (Descriptor) method.invoke(null);
        } catch (NoSuchMethodException e) {
            return null;
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    private ColumnValueDescription(Format format,
                                   String className,
                                   String canonicalClassName,
                                   Compression compression,
                                   Descriptor protoDescriptor) {
        this.compression = Preconditions.checkNotNull(compression);
        this.type = ValueType.BLOB;
        this.format = Preconditions.checkNotNull(format);
        Validate.notEmpty(className);
        Validate.notEmpty(canonicalClassName);
        Validate.isTrue(format != Format.VALUE_TYPE);

        switch (format) {
        case PERSISTABLE:
        case PROTO:
            this.canonicalClassName = Preconditions.checkNotNull(canonicalClassName);
            this.className = Preconditions.checkNotNull(className);
            this.protoDescriptor = protoDescriptor;
            break;
        default:
            throw new IllegalArgumentException();
        }
    }

    public int getMaxValueSize() {
        return type.getMaxValueSize();
    }

    public ValueType getValueType() {
        return type;
    }

    public Compression getCompression() {
        return compression;
    }

    public Format getFormat() {
        return format;
    }

    public Descriptor getProtoDescriptor() {
        return protoDescriptor;
    }

    /**
     * This gets a string that represents the canonical name of the class that is stored in this column value.
     */
    public String getJavaObjectTypeName() {
        if (canonicalClassName != null) {
            return canonicalClassName;
        }
        return type.getJavaObjectClassName();
    }

    public String getPersistCode(String varName) {
        final String result;
        if (format == Format.PERSISTABLE) {
            result = varName + ".persistToBytes()";
        } else if (format == Format.PROTO) {
            result = varName + ".toByteArray()";
        } else {
            result = type.getPersistCode(varName);
        }
        return "com.palantir.atlasdb.compress.CompressionUtils.compress(" + result + ", " +
                "com.palantir.atlasdb.table.description.ColumnValueDescription.Compression." + compression + ")";
    }

    public byte[] persistJsonToBytes(String str) throws ParseException {
        return persistJsonToBytes(Thread.currentThread().getContextClassLoader(), str);
    }

    @SuppressWarnings("unchecked")
    public byte[] persistJsonToBytes(ClassLoader classLoader, String str) throws ParseException {
        final byte[] bytes;
        if (format == Format.PERSISTABLE) {
            Class<?> clazz = getImportClass(classLoader);
            Hydrator<? extends JsonPersistable> hydrator = null;
            if (JsonPersistable.class.isAssignableFrom(clazz)) {
                hydrator = JsonPersistables.getHydrator((Class<? extends JsonPersistable>) clazz);
            }
            if (hydrator == null) {
                throw new IllegalArgumentException("Tried to write a Persistable object that is not a valid JsonPersistable");
            }
            bytes = hydrator.hydrateFromJson(str).persistToBytes();
        } else if (format == Format.PROTO) {
            GeneratedMessage.Builder<?> builder = createBuilder(classLoader);
            // This will have issues with
            JsonFormat.merge(str, builder);
            bytes = builder.build().toByteArray();
        } else {
            bytes = type.convertFromString(str);
        }
        return CompressionUtils.compress(bytes, compression);
    }

    private GeneratedMessage.Builder<?> createBuilder(ClassLoader classLoader) {
        try {
            Method method = getImportClass(classLoader).getMethod("newBuilder");
            return (GeneratedMessage.Builder<?>) method.invoke(null);
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    public Class<?> getImportClass() {
        return getImportClass(Thread.currentThread().getContextClassLoader());
    }

    public Class<?> getImportClass(ClassLoader classLoader) {
        if (className == null) {
            return type.getTypeClass();
        }
        try {
            return Class.forName(className, true, classLoader);
        } catch (ClassNotFoundException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    public String getHydrateCode(String varName) {
        varName = "com.palantir.atlasdb.compress.CompressionUtils.decompress(" + varName + ", com.palantir.atlasdb.table.description.ColumnValueDescription.Compression." + compression + ")";
        if (format == Format.PERSISTABLE) {
            return canonicalClassName + "." + Persistable.HYDRATOR_NAME + ".hydrateFromBytes(" + varName + ")";
        } else if (format == Format.PROTO) {
                return "new Supplier<" + canonicalClassName + ">() { " +
                    "@Override " +
                    "public " + canonicalClassName + " get() { " +
                        "try { " +
                            "return " + canonicalClassName + ".parseFrom(" + varName + "); " +
                        "} catch (InvalidProtocolBufferException _ex) { " +
                            "throw Throwables.throwUncheckedException(_ex); " +
                        "} " +
                    "} " +
                "}.get()";
        } else {
            return type.getHydrateCode(varName, "0");
        }
    }

    @SuppressWarnings("unchecked")
    public Message hydrateProto(ClassLoader classLoader, byte[] value) {
        Preconditions.checkState(format == Format.PROTO, "Column value is not a protocol buffer.");
        return ColumnValues.parseProtoBuf((Class<? extends GeneratedMessage>)getImportClass(classLoader), CompressionUtils.decompress(value, compression));
    }

    @SuppressWarnings("unchecked")
    public Persistable hydratePersistable(ClassLoader classLoader, byte[] value) {
        Preconditions.checkState(format == Format.PERSISTABLE, "Column value is not a Persistable.");
        return ColumnValues.parsePersistable((Class<? extends Persistable>)getImportClass(classLoader), CompressionUtils.decompress(value, compression));
    }


    public TableMetadataPersistence.ColumnValueDescription.Builder persistToProto() {
        Builder builder = TableMetadataPersistence.ColumnValueDescription.newBuilder();
        builder.setType(type.persistToProto());
        builder.setCompression(compression.persistToProto());
        if (className != null) {
            builder.setClassName(className);
        }
        if (canonicalClassName != null) {
            builder.setCanonicalClassName(canonicalClassName);
        }
        builder.setFormat(format.persistToProto());
        if (protoDescriptor != null) {
            builder.setProtoFileDescriptor(protoDescriptor.getFile().toProto().toByteString());
            builder.setProtoMessageName(protoDescriptor.getName());
            if (protoDescriptor.getContainingType() != null) {
                log.error("proto descriptors should be top level types: " + protoDescriptor.getName());
            }
            if (!protoDescriptor.getFile().getDependencies().isEmpty()) {
                log.error("We don't currently support protos that have import dependencies:"
                        + protoDescriptor.getFile().getName());
            }
        }
        return builder;
    }

    public static ColumnValueDescription hydrateFromProto(TableMetadataPersistence.ColumnValueDescription message) {
        ValueType type = ValueType.hydrateFromProto(message.getType());
        Compression compression = Compression.hydrateFromProto(message.getCompression());
        if (!message.hasClassName()) {
            return new ColumnValueDescription(type, compression);
        }

        Validate.isTrue(type == ValueType.BLOB);
        if (message.hasFormat()) {
            Format format = Format.hydrateFromProto(message.getFormat());
            Descriptor protoDescriptor = null;
            if (message.hasProtoFileDescriptor()) {
                try {
                    FileDescriptorProto fileProto = FileDescriptorProto.parseFrom(message.getProtoFileDescriptor());
                    FileDescriptor fileDescriptor = FileDescriptor.buildFrom(fileProto, new FileDescriptor[0]);
                    protoDescriptor = fileDescriptor.findMessageTypeByName(message.getProtoMessageName());
                } catch (Exception e) {
                    log.warn("Failed to parse FileDescriptorProto.", e);
                }
            }
            return new ColumnValueDescription(
                    format,
                    message.getClassName(),
                    message.getCanonicalClassName(),
                    compression,
                    protoDescriptor);
        }

        /*
         * All the code in the rest of this method is to support old protos that don't have a format field.
         * Format and canonicalClassName were added at the same time.
         *
         * Once we upgrade all the old protos (after 3.6.0), we can remove the below code.
         */

        final Class<?> clazz;
        try {
            clazz = Class.forName(message.getClassName());
        } catch (ClassNotFoundException e) {
            throw Throwables.throwUncheckedException(e);
        }
        final Format format;
        if (GeneratedMessage.class.isAssignableFrom(clazz)) {
            format = Format.PROTO;
        } else {
            format = Format.PERSISTABLE;
        }
        return new ColumnValueDescription(format, message.getClassName(), clazz.getCanonicalName(), compression, null);
    }

    @Override
    public String toString() {
        return "ColumnValueDescription [format=" + format + ", compression=" + compression
                + ", type=" + type + ", className=" + className + ", canonicalClassName="
                + canonicalClassName + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;

        int result = 1;
        result = prime * result + (format == null ? 0 : format.hashCode());
        result = prime * result + (compression == null ? 0 : compression.hashCode());
        result = prime * result + (type == null ? 0 : type.hashCode());
        result = prime * result + (className == null ? 0 : className.hashCode());
        result = prime * result + (canonicalClassName == null ? 0 : canonicalClassName.hashCode());
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
        ColumnValueDescription other = (ColumnValueDescription) obj;
        if (format == null) {
            if (other.getFormat() != null) {
                return false;
            }
        } else if (!format.equals(other.getFormat())) {
            return false;
        }
        if (compression == null) {
            if (other.getCompression() != null) {
                return false;
            }
        } else if (!compression.equals(other.getCompression())) {
            return false;
        }
        if (type == null) {
            if (other.type != null) {
                return false;
            }
        } else if (!type.equals(other.type)) {
            return false;
        }
        if (className == null) {
            if (other.className != null) {
                return false;
            }
        } else if (!className.equals(other.className)) {
            return false;
        }
        if (canonicalClassName == null) {
            if (other.canonicalClassName != null) {
                return false;
            }
        } else if (!canonicalClassName.equals(other.canonicalClassName)) {
            return false;
        }
        return true;
    }
}
