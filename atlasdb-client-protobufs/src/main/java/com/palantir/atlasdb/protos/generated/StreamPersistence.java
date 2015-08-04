/**
 * // Copyright 2015 Palantir Technologies
 * //
 * // Licensed under the BSD-3 License (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * // http://opensource.org/licenses/BSD-3-Clause
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */
package com.palantir.atlasdb.protos.generated;

public final class StreamPersistence {
  private StreamPersistence() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
  }
  /**
   * Protobuf enum {@code com.palantir.atlasdb.protos.generated.Status}
   */
  public enum Status
      implements com.google.protobuf.ProtocolMessageEnum {
    /**
     * <code>STORING = 1;</code>
     */
    STORING(0, 1),
    /**
     * <code>STORED = 2;</code>
     */
    STORED(1, 2),
    /**
     * <code>FAILED = 3;</code>
     */
    FAILED(2, 3),
    ;

    /**
     * <code>STORING = 1;</code>
     */
    public static final int STORING_VALUE = 1;
    /**
     * <code>STORED = 2;</code>
     */
    public static final int STORED_VALUE = 2;
    /**
     * <code>FAILED = 3;</code>
     */
    public static final int FAILED_VALUE = 3;


    public final int getNumber() { return value; }

    public static Status valueOf(int value) {
      switch (value) {
        case 1: return STORING;
        case 2: return STORED;
        case 3: return FAILED;
        default: return null;
      }
    }

    public static com.google.protobuf.Internal.EnumLiteMap<Status>
        internalGetValueMap() {
      return internalValueMap;
    }
    private static com.google.protobuf.Internal.EnumLiteMap<Status>
        internalValueMap =
          new com.google.protobuf.Internal.EnumLiteMap<Status>() {
            public Status findValueByNumber(int number) {
              return Status.valueOf(number);
            }
          };

    public final com.google.protobuf.Descriptors.EnumValueDescriptor
        getValueDescriptor() {
      return getDescriptor().getValues().get(index);
    }
    public final com.google.protobuf.Descriptors.EnumDescriptor
        getDescriptorForType() {
      return getDescriptor();
    }
    public static final com.google.protobuf.Descriptors.EnumDescriptor
        getDescriptor() {
      return com.palantir.atlasdb.protos.generated.StreamPersistence.getDescriptor().getEnumTypes().get(0);
    }

    private static final Status[] VALUES = values();

    public static Status valueOf(
        com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
      if (desc.getType() != getDescriptor()) {
        throw new java.lang.IllegalArgumentException(
          "EnumValueDescriptor is not for this type.");
      }
      return VALUES[desc.getIndex()];
    }

    private final int index;
    private final int value;

    private Status(int index, int value) {
      this.index = index;
      this.value = value;
    }

    // @@protoc_insertion_point(enum_scope:com.palantir.atlasdb.protos.generated.Status)
  }

  public interface StreamMetadataOrBuilder extends
      // @@protoc_insertion_point(interface_extends:com.palantir.atlasdb.protos.generated.StreamMetadata)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>required .com.palantir.atlasdb.protos.generated.Status status = 1;</code>
     */
    boolean hasStatus();
    /**
     * <code>required .com.palantir.atlasdb.protos.generated.Status status = 1;</code>
     */
    com.palantir.atlasdb.protos.generated.StreamPersistence.Status getStatus();

    /**
     * <code>required int64 length = 2;</code>
     */
    boolean hasLength();
    /**
     * <code>required int64 length = 2;</code>
     */
    long getLength();

    /**
     * <code>required bytes hash = 3;</code>
     */
    boolean hasHash();
    /**
     * <code>required bytes hash = 3;</code>
     */
    com.google.protobuf.ByteString getHash();
  }
  /**
   * Protobuf type {@code com.palantir.atlasdb.protos.generated.StreamMetadata}
   */
  public static final class StreamMetadata extends
      com.google.protobuf.GeneratedMessage implements
      // @@protoc_insertion_point(message_implements:com.palantir.atlasdb.protos.generated.StreamMetadata)
      StreamMetadataOrBuilder {
    // Use StreamMetadata.newBuilder() to construct.
    private StreamMetadata(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private StreamMetadata(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final StreamMetadata defaultInstance;
    public static StreamMetadata getDefaultInstance() {
      return defaultInstance;
    }

    public StreamMetadata getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private StreamMetadata(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 8: {
              int rawValue = input.readEnum();
              com.palantir.atlasdb.protos.generated.StreamPersistence.Status value = com.palantir.atlasdb.protos.generated.StreamPersistence.Status.valueOf(rawValue);
              if (value == null) {
                unknownFields.mergeVarintField(1, rawValue);
              } else {
                bitField0_ |= 0x00000001;
                status_ = value;
              }
              break;
            }
            case 16: {
              bitField0_ |= 0x00000002;
              length_ = input.readInt64();
              break;
            }
            case 26: {
              bitField0_ |= 0x00000004;
              hash_ = input.readBytes();
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.palantir.atlasdb.protos.generated.StreamPersistence.internal_static_com_palantir_atlasdb_protos_generated_StreamMetadata_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.palantir.atlasdb.protos.generated.StreamPersistence.internal_static_com_palantir_atlasdb_protos_generated_StreamMetadata_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata.class, com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata.Builder.class);
    }

    public static com.google.protobuf.Parser<StreamMetadata> PARSER =
        new com.google.protobuf.AbstractParser<StreamMetadata>() {
      public StreamMetadata parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new StreamMetadata(input, extensionRegistry);
      }
    };

    @java.lang.Override
    public com.google.protobuf.Parser<StreamMetadata> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    public static final int STATUS_FIELD_NUMBER = 1;
    private com.palantir.atlasdb.protos.generated.StreamPersistence.Status status_;
    /**
     * <code>required .com.palantir.atlasdb.protos.generated.Status status = 1;</code>
     */
    public boolean hasStatus() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>required .com.palantir.atlasdb.protos.generated.Status status = 1;</code>
     */
    public com.palantir.atlasdb.protos.generated.StreamPersistence.Status getStatus() {
      return status_;
    }

    public static final int LENGTH_FIELD_NUMBER = 2;
    private long length_;
    /**
     * <code>required int64 length = 2;</code>
     */
    public boolean hasLength() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>required int64 length = 2;</code>
     */
    public long getLength() {
      return length_;
    }

    public static final int HASH_FIELD_NUMBER = 3;
    private com.google.protobuf.ByteString hash_;
    /**
     * <code>required bytes hash = 3;</code>
     */
    public boolean hasHash() {
      return ((bitField0_ & 0x00000004) == 0x00000004);
    }
    /**
     * <code>required bytes hash = 3;</code>
     */
    public com.google.protobuf.ByteString getHash() {
      return hash_;
    }

    private void initFields() {
      status_ = com.palantir.atlasdb.protos.generated.StreamPersistence.Status.STORING;
      length_ = 0L;
      hash_ = com.google.protobuf.ByteString.EMPTY;
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      if (!hasStatus()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (!hasLength()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (!hasHash()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeEnum(1, status_.getNumber());
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        output.writeInt64(2, length_);
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        output.writeBytes(3, hash_);
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeEnumSize(1, status_.getNumber());
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt64Size(2, length_);
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(3, hash_);
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @java.lang.Override
    protected java.lang.Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    public static com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code com.palantir.atlasdb.protos.generated.StreamMetadata}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:com.palantir.atlasdb.protos.generated.StreamMetadata)
        com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadataOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return com.palantir.atlasdb.protos.generated.StreamPersistence.internal_static_com_palantir_atlasdb_protos_generated_StreamMetadata_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return com.palantir.atlasdb.protos.generated.StreamPersistence.internal_static_com_palantir_atlasdb_protos_generated_StreamMetadata_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata.class, com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata.Builder.class);
      }

      // Construct using com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessage.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        status_ = com.palantir.atlasdb.protos.generated.StreamPersistence.Status.STORING;
        bitField0_ = (bitField0_ & ~0x00000001);
        length_ = 0L;
        bitField0_ = (bitField0_ & ~0x00000002);
        hash_ = com.google.protobuf.ByteString.EMPTY;
        bitField0_ = (bitField0_ & ~0x00000004);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return com.palantir.atlasdb.protos.generated.StreamPersistence.internal_static_com_palantir_atlasdb_protos_generated_StreamMetadata_descriptor;
      }

      public com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata getDefaultInstanceForType() {
        return com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata.getDefaultInstance();
      }

      public com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata build() {
        com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata buildPartial() {
        com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata result = new com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.status_ = status_;
        if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
          to_bitField0_ |= 0x00000002;
        }
        result.length_ = length_;
        if (((from_bitField0_ & 0x00000004) == 0x00000004)) {
          to_bitField0_ |= 0x00000004;
        }
        result.hash_ = hash_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata) {
          return mergeFrom((com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata other) {
        if (other == com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata.getDefaultInstance()) return this;
        if (other.hasStatus()) {
          setStatus(other.getStatus());
        }
        if (other.hasLength()) {
          setLength(other.getLength());
        }
        if (other.hasHash()) {
          setHash(other.getHash());
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        if (!hasStatus()) {
          
          return false;
        }
        if (!hasLength()) {
          
          return false;
        }
        if (!hasHash()) {
          
          return false;
        }
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      private com.palantir.atlasdb.protos.generated.StreamPersistence.Status status_ = com.palantir.atlasdb.protos.generated.StreamPersistence.Status.STORING;
      /**
       * <code>required .com.palantir.atlasdb.protos.generated.Status status = 1;</code>
       */
      public boolean hasStatus() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>required .com.palantir.atlasdb.protos.generated.Status status = 1;</code>
       */
      public com.palantir.atlasdb.protos.generated.StreamPersistence.Status getStatus() {
        return status_;
      }
      /**
       * <code>required .com.palantir.atlasdb.protos.generated.Status status = 1;</code>
       */
      public Builder setStatus(com.palantir.atlasdb.protos.generated.StreamPersistence.Status value) {
        if (value == null) {
          throw new NullPointerException();
        }
        bitField0_ |= 0x00000001;
        status_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required .com.palantir.atlasdb.protos.generated.Status status = 1;</code>
       */
      public Builder clearStatus() {
        bitField0_ = (bitField0_ & ~0x00000001);
        status_ = com.palantir.atlasdb.protos.generated.StreamPersistence.Status.STORING;
        onChanged();
        return this;
      }

      private long length_ ;
      /**
       * <code>required int64 length = 2;</code>
       */
      public boolean hasLength() {
        return ((bitField0_ & 0x00000002) == 0x00000002);
      }
      /**
       * <code>required int64 length = 2;</code>
       */
      public long getLength() {
        return length_;
      }
      /**
       * <code>required int64 length = 2;</code>
       */
      public Builder setLength(long value) {
        bitField0_ |= 0x00000002;
        length_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required int64 length = 2;</code>
       */
      public Builder clearLength() {
        bitField0_ = (bitField0_ & ~0x00000002);
        length_ = 0L;
        onChanged();
        return this;
      }

      private com.google.protobuf.ByteString hash_ = com.google.protobuf.ByteString.EMPTY;
      /**
       * <code>required bytes hash = 3;</code>
       */
      public boolean hasHash() {
        return ((bitField0_ & 0x00000004) == 0x00000004);
      }
      /**
       * <code>required bytes hash = 3;</code>
       */
      public com.google.protobuf.ByteString getHash() {
        return hash_;
      }
      /**
       * <code>required bytes hash = 3;</code>
       */
      public Builder setHash(com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000004;
        hash_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required bytes hash = 3;</code>
       */
      public Builder clearHash() {
        bitField0_ = (bitField0_ & ~0x00000004);
        hash_ = getDefaultInstance().getHash();
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:com.palantir.atlasdb.protos.generated.StreamMetadata)
    }

    static {
      defaultInstance = new StreamMetadata(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:com.palantir.atlasdb.protos.generated.StreamMetadata)
  }

  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_com_palantir_atlasdb_protos_generated_StreamMetadata_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_com_palantir_atlasdb_protos_generated_StreamMetadata_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n3com/palantir/atlasdb/protos/StreamPers" +
      "istence.proto\022%com.palantir.atlasdb.prot" +
      "os.generated\"m\n\016StreamMetadata\022=\n\006status" +
      "\030\001 \002(\0162-.com.palantir.atlasdb.protos.gen" +
      "erated.Status\022\016\n\006length\030\002 \002(\003\022\014\n\004hash\030\003 " +
      "\002(\014*-\n\006Status\022\013\n\007STORING\020\001\022\n\n\006STORED\020\002\022\n" +
      "\n\006FAILED\020\003"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
        new com.google.protobuf.Descriptors.FileDescriptor.    InternalDescriptorAssigner() {
          public com.google.protobuf.ExtensionRegistry assignDescriptors(
              com.google.protobuf.Descriptors.FileDescriptor root) {
            descriptor = root;
            return null;
          }
        };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        }, assigner);
    internal_static_com_palantir_atlasdb_protos_generated_StreamMetadata_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_com_palantir_atlasdb_protos_generated_StreamMetadata_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessage.FieldAccessorTable(
        internal_static_com_palantir_atlasdb_protos_generated_StreamMetadata_descriptor,
        new java.lang.String[] { "Status", "Length", "Hash", });
  }

  // @@protoc_insertion_point(outer_class_scope)
}
