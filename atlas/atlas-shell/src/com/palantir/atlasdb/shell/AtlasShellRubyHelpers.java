package com.palantir.atlasdb.shell;

import java.math.BigInteger;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.googlecode.protobuf.format.JsonFormat;
import com.googlecode.protobuf.format.JsonFormat.ParseException;
import com.palantir.atlasdb.shell.AtlasShellRubyHelpers.JsonFormatAdaptor.TokenizerAdaptor;
import com.palantir.common.persist.Persistable;
import com.palantir.atlasdb.compress.CompressionUtils;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.ColumnValueDescription.Compression;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.proto.fork.ForkedJsonFormat;
import com.palantir.ptoss.util.Throwables;

/**
 * A few miscellaneous functions that must be available from inside Ruby but that do not depend on
 * the state of the outside world.
 */
public class AtlasShellRubyHelpers {

    private AtlasShellRubyHelpers() {
        // empty
    }

    public static byte[] getEndRowForPrefixRange(byte[] row) {
        return RangeRequest.builder().prefixRange(row).build().getEndExclusive();
    }

    public static String formatNamedColumnValue(NamedColumnDescription ncd, RowResult<byte[]> row) {
        byte[] value = row.getColumns().get(ncd.getShortName().getBytes());
        return value != null ? formatColumnValue(ncd.getValue(), value) : null;
    }

    public static String convertJsonByteStringIntoHex(String jsonByteString) throws ParseException {
        TokenizerAdaptor tokenizer = new TokenizerAdaptor(jsonByteString);
        ByteString bytes = tokenizer.consumeByteString();
        return PtBytes.encodeBase64String(bytes.toByteArray());
    }

    public static class JsonFormatAdaptor extends JsonFormat {
        public static class TokenizerAdaptor extends Tokenizer {
            public TokenizerAdaptor(CharSequence chars) {
                super(chars);
            }
        }
    }

    public static String formatColumnValue(ColumnValueDescription columnValueDescription,
                                           byte[] rawValue) {
        if (rawValue == null) {
            return null;
        }
        switch (columnValueDescription.getFormat()) {
        case PROTO:
            Descriptor protoDescriptor = columnValueDescription.getProtoDescriptor();
            if (protoDescriptor != null) {
                try {
                    Compression compression = columnValueDescription.getCompression();
                    byte[] uncompressed = CompressionUtils.decompress(rawValue, compression);
                    DynamicMessage dynamicMessage = DynamicMessage.parseFrom(
                            protoDescriptor,
                            uncompressed);
                    String theAllegedAnswer = ForkedJsonFormat.printToString(dynamicMessage);
                    return ProtobufJavaFormatWorkaround.cleanupJsonWithInvalidEscapes(theAllegedAnswer);
                } catch (InvalidProtocolBufferException e) {
                    throw Throwables.throwUncheckedException(e);
                }
            }
            return PtBytes.encodeBase64String(rawValue);
        case VALUE_TYPE:
            // TODO: this should support non-string types
            return columnValueDescription.getValueType().convertToString(rawValue);
        case PERSISTABLE:
            ClassLoader classLoader = AtlasShellRubyHelpers.class.getClassLoader();
            Persistable persistable = columnValueDescription.hydratePersistable(
                    classLoader,
                    rawValue);
            return String.valueOf(persistable);
        default:
            return PtBytes.encodeBase64String(rawValue);
        }
    }

    /**
     * @param json a String representation of a JSON object
     * @return a canonical, easy to read String representation of a JSON object
     */
    public static String prettyPrintJson(String json) {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        JsonParser jp = new JsonParser();
        JsonElement je = jp.parse(json);
        return gson.toJson(je);
    }

    /**
     * @param json a String representation of a JSON object, or perhaps not
     * @return whether the given String is indeed a syntactically correct JSON expression
     */
    public static boolean isJsonObject(String json) {
        try {
            JsonElement elem = new JsonParser().parse(json);
            return elem.isJsonObject();
        } catch (JsonParseException e) {
            return false;
        }
    }

    /**
     * Helper to return the results as a Map or a String. Most of this code is adapted from
     * {@link JsonFormat}
     */
    public static class ProtoToMapHelper {
        private ProtoToMapHelper() {
            //
        }

        public static Object asMapOrString(ColumnValueDescription cvd, byte[] value) {
            if (value == null) {
                return null;
            }

            switch (cvd.getFormat()) {
            case PROTO:
                Descriptor protoDescriptor = cvd.getProtoDescriptor();
                if (protoDescriptor != null) {
                    try {
                        Compression compression = cvd.getCompression();
                        byte[] uncompressed = CompressionUtils.decompress(value, compression);
                        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(
                                protoDescriptor,
                                uncompressed);

                        return messageToMap(dynamicMessage);
                    } catch (InvalidProtocolBufferException e) {
                        throw Throwables.throwUncheckedException(e);
                    }
                }
                return PtBytes.encodeBase64String(value);
            default:
                return formatColumnValue(cvd, value);
            }
        }

        public static Object asMapOrString(NamedColumnDescription ncd, RowResult<byte[]> row) {
            byte[] value = row.getColumns().get(ncd.getShortName().getBytes());
            return value != null ? asMapOrString(ncd.getValue(), value) : null;
        }

        private static Object messageToMap(DynamicMessage dynamicMessage) {
            Map<String, Object> ret = Maps.newHashMap();

            for (Map.Entry<FieldDescriptor, Object> e : dynamicMessage.getAllFields().entrySet()) {
                FieldDescriptor field = e.getKey();
                Object obj = e.getValue();
                addToMap(field, obj, ret);
            }
            if (dynamicMessage.getUnknownFields().asMap().size() > 0) {
                ret.put("UNKNOWN_FIELDS", JsonFormat.printToString(dynamicMessage.getUnknownFields()));
            }

            return ret;
        }

        private static void addToMap(FieldDescriptor field, Object obj, Map<String, Object> map) {
            String key;
            if (field.isExtension()) {
                // We special-case MessageSet elements for compatibility with proto1.
                if (field.getContainingType().getOptions().getMessageSetWireFormat()
                    && (field.getType() == FieldDescriptor.Type.MESSAGE) && (field.isOptional())
                    // object equality
                    && (field.getExtensionScope() == field.getMessageType())) {
                    key = field.getMessageType().getFullName();
                } else {
                    key = field.getFullName();
                }
            } else {
                if (field.getType() == FieldDescriptor.Type.GROUP) {
                    // Groups must be serialized with their original capitalization.
                    key = field.getMessageType().getName();
                } else {
                    key = field.getName();
                }
            }

            Object value;
            if (field.isRepeated()) {
                List<Object> list = Lists.newArrayList();
                for (Iterator<?> iter = ((List<?>) obj).iterator(); iter.hasNext();) {
                    list.add(getValue(field, iter.next()));
                }
                value = list;
            } else {
                value = getValue(field, obj);
            }

            map.put(key, value);
        }

        private static Object getValue(FieldDescriptor field, Object value) {
            switch (field.getType()) {
                case INT32:
                case INT64:
                case SINT32:
                case SINT64:
                case SFIXED32:
                case SFIXED64:
                case FLOAT:
                case DOUBLE:
                case BOOL:
                    return value;

                case UINT32:
                case FIXED32:
                    return unsignedToLong((Integer) value);

                case UINT64:
                case FIXED64:
                    return unsignedToLongOrBigInteger((Long) value);

                case STRING:
                    return value;

                case BYTES:
                    return PtBytes.encodeBase64String(((ByteString) value).toByteArray());

                case ENUM:
                    return ((EnumValueDescriptor) value).getName();

                case MESSAGE:
                case GROUP:
                    return messageToMap((DynamicMessage) value);
            }
            return "Unknown proto type " + field.getType();
        }

        private static long unsignedToLong(int value) {
            if (value >= 0) {
                return value;
            } else {
                return value & 0x00000000FFFFFFFFL;
            }
        }

        /**
         * Convert an unsigned 64-bit integer to a long or BigInteger.
         */
        private static Object unsignedToLongOrBigInteger(long value) {
            if (value >= 0) {
                return value;
            } else {
                // Pull off the most-significant bit so that BigInteger doesn't think
                // the number is negative, then set it again using setBit().
                return BigInteger.valueOf(value & 0x7FFFFFFFFFFFFFFFL).setBit(63);
            }
        }
    }
}
