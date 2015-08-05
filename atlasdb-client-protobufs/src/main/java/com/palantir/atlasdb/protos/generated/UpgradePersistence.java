/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.protos.generated;

public final class UpgradePersistence {
  private UpgradePersistence() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
  }
  public interface SchemaVersionOrBuilder extends
      // @@protoc_insertion_point(interface_extends:com.palantir.atlasdb.protos.generated.SchemaVersion)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>required int64 version = 1;</code>
     */
    boolean hasVersion();
    /**
     * <code>required int64 version = 1;</code>
     */
    long getVersion();

    /**
     * <code>optional int64 hotfix = 2;</code>
     */
    boolean hasHotfix();
    /**
     * <code>optional int64 hotfix = 2;</code>
     */
    long getHotfix();

    /**
     * <code>optional int64 hotfix_hotfix = 3;</code>
     */
    boolean hasHotfixHotfix();
    /**
     * <code>optional int64 hotfix_hotfix = 3;</code>
     */
    long getHotfixHotfix();
  }
  /**
   * Protobuf type {@code com.palantir.atlasdb.protos.generated.SchemaVersion}
   */
  public static final class SchemaVersion extends
      com.google.protobuf.GeneratedMessage implements
      // @@protoc_insertion_point(message_implements:com.palantir.atlasdb.protos.generated.SchemaVersion)
      SchemaVersionOrBuilder {
    // Use SchemaVersion.newBuilder() to construct.
    private SchemaVersion(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private SchemaVersion(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final SchemaVersion defaultInstance;
    public static SchemaVersion getDefaultInstance() {
      return defaultInstance;
    }

    public SchemaVersion getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private SchemaVersion(
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
              bitField0_ |= 0x00000001;
              version_ = input.readInt64();
              break;
            }
            case 16: {
              bitField0_ |= 0x00000002;
              hotfix_ = input.readInt64();
              break;
            }
            case 24: {
              bitField0_ |= 0x00000004;
              hotfixHotfix_ = input.readInt64();
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
      return com.palantir.atlasdb.protos.generated.UpgradePersistence.internal_static_com_palantir_atlasdb_protos_generated_SchemaVersion_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.palantir.atlasdb.protos.generated.UpgradePersistence.internal_static_com_palantir_atlasdb_protos_generated_SchemaVersion_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion.class, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion.Builder.class);
    }

    public static com.google.protobuf.Parser<SchemaVersion> PARSER =
        new com.google.protobuf.AbstractParser<SchemaVersion>() {
      public SchemaVersion parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new SchemaVersion(input, extensionRegistry);
      }
    };

    @java.lang.Override
    public com.google.protobuf.Parser<SchemaVersion> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    public static final int VERSION_FIELD_NUMBER = 1;
    private long version_;
    /**
     * <code>required int64 version = 1;</code>
     */
    public boolean hasVersion() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>required int64 version = 1;</code>
     */
    public long getVersion() {
      return version_;
    }

    public static final int HOTFIX_FIELD_NUMBER = 2;
    private long hotfix_;
    /**
     * <code>optional int64 hotfix = 2;</code>
     */
    public boolean hasHotfix() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>optional int64 hotfix = 2;</code>
     */
    public long getHotfix() {
      return hotfix_;
    }

    public static final int HOTFIX_HOTFIX_FIELD_NUMBER = 3;
    private long hotfixHotfix_;
    /**
     * <code>optional int64 hotfix_hotfix = 3;</code>
     */
    public boolean hasHotfixHotfix() {
      return ((bitField0_ & 0x00000004) == 0x00000004);
    }
    /**
     * <code>optional int64 hotfix_hotfix = 3;</code>
     */
    public long getHotfixHotfix() {
      return hotfixHotfix_;
    }

    private void initFields() {
      version_ = 0L;
      hotfix_ = 0L;
      hotfixHotfix_ = 0L;
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      if (!hasVersion()) {
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
        output.writeInt64(1, version_);
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        output.writeInt64(2, hotfix_);
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        output.writeInt64(3, hotfixHotfix_);
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
          .computeInt64Size(1, version_);
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt64Size(2, hotfix_);
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt64Size(3, hotfixHotfix_);
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

    public static com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion prototype) {
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
     * Protobuf type {@code com.palantir.atlasdb.protos.generated.SchemaVersion}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:com.palantir.atlasdb.protos.generated.SchemaVersion)
        com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersionOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return com.palantir.atlasdb.protos.generated.UpgradePersistence.internal_static_com_palantir_atlasdb_protos_generated_SchemaVersion_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return com.palantir.atlasdb.protos.generated.UpgradePersistence.internal_static_com_palantir_atlasdb_protos_generated_SchemaVersion_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion.class, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion.Builder.class);
      }

      // Construct using com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion.newBuilder()
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
        version_ = 0L;
        bitField0_ = (bitField0_ & ~0x00000001);
        hotfix_ = 0L;
        bitField0_ = (bitField0_ & ~0x00000002);
        hotfixHotfix_ = 0L;
        bitField0_ = (bitField0_ & ~0x00000004);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return com.palantir.atlasdb.protos.generated.UpgradePersistence.internal_static_com_palantir_atlasdb_protos_generated_SchemaVersion_descriptor;
      }

      public com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion getDefaultInstanceForType() {
        return com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion.getDefaultInstance();
      }

      public com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion build() {
        com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion buildPartial() {
        com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion result = new com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.version_ = version_;
        if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
          to_bitField0_ |= 0x00000002;
        }
        result.hotfix_ = hotfix_;
        if (((from_bitField0_ & 0x00000004) == 0x00000004)) {
          to_bitField0_ |= 0x00000004;
        }
        result.hotfixHotfix_ = hotfixHotfix_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion) {
          return mergeFrom((com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion other) {
        if (other == com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion.getDefaultInstance()) return this;
        if (other.hasVersion()) {
          setVersion(other.getVersion());
        }
        if (other.hasHotfix()) {
          setHotfix(other.getHotfix());
        }
        if (other.hasHotfixHotfix()) {
          setHotfixHotfix(other.getHotfixHotfix());
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        if (!hasVersion()) {
          
          return false;
        }
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      private long version_ ;
      /**
       * <code>required int64 version = 1;</code>
       */
      public boolean hasVersion() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>required int64 version = 1;</code>
       */
      public long getVersion() {
        return version_;
      }
      /**
       * <code>required int64 version = 1;</code>
       */
      public Builder setVersion(long value) {
        bitField0_ |= 0x00000001;
        version_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required int64 version = 1;</code>
       */
      public Builder clearVersion() {
        bitField0_ = (bitField0_ & ~0x00000001);
        version_ = 0L;
        onChanged();
        return this;
      }

      private long hotfix_ ;
      /**
       * <code>optional int64 hotfix = 2;</code>
       */
      public boolean hasHotfix() {
        return ((bitField0_ & 0x00000002) == 0x00000002);
      }
      /**
       * <code>optional int64 hotfix = 2;</code>
       */
      public long getHotfix() {
        return hotfix_;
      }
      /**
       * <code>optional int64 hotfix = 2;</code>
       */
      public Builder setHotfix(long value) {
        bitField0_ |= 0x00000002;
        hotfix_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional int64 hotfix = 2;</code>
       */
      public Builder clearHotfix() {
        bitField0_ = (bitField0_ & ~0x00000002);
        hotfix_ = 0L;
        onChanged();
        return this;
      }

      private long hotfixHotfix_ ;
      /**
       * <code>optional int64 hotfix_hotfix = 3;</code>
       */
      public boolean hasHotfixHotfix() {
        return ((bitField0_ & 0x00000004) == 0x00000004);
      }
      /**
       * <code>optional int64 hotfix_hotfix = 3;</code>
       */
      public long getHotfixHotfix() {
        return hotfixHotfix_;
      }
      /**
       * <code>optional int64 hotfix_hotfix = 3;</code>
       */
      public Builder setHotfixHotfix(long value) {
        bitField0_ |= 0x00000004;
        hotfixHotfix_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional int64 hotfix_hotfix = 3;</code>
       */
      public Builder clearHotfixHotfix() {
        bitField0_ = (bitField0_ & ~0x00000004);
        hotfixHotfix_ = 0L;
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:com.palantir.atlasdb.protos.generated.SchemaVersion)
    }

    static {
      defaultInstance = new SchemaVersion(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:com.palantir.atlasdb.protos.generated.SchemaVersion)
  }

  public interface SchemaVersionsOrBuilder extends
      // @@protoc_insertion_point(interface_extends:com.palantir.atlasdb.protos.generated.SchemaVersions)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>repeated .com.palantir.atlasdb.protos.generated.SchemaVersion versions = 1;</code>
     */
    java.util.List<com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion> 
        getVersionsList();
    /**
     * <code>repeated .com.palantir.atlasdb.protos.generated.SchemaVersion versions = 1;</code>
     */
    com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion getVersions(int index);
    /**
     * <code>repeated .com.palantir.atlasdb.protos.generated.SchemaVersion versions = 1;</code>
     */
    int getVersionsCount();
    /**
     * <code>repeated .com.palantir.atlasdb.protos.generated.SchemaVersion versions = 1;</code>
     */
    java.util.List<? extends com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersionOrBuilder> 
        getVersionsOrBuilderList();
    /**
     * <code>repeated .com.palantir.atlasdb.protos.generated.SchemaVersion versions = 1;</code>
     */
    com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersionOrBuilder getVersionsOrBuilder(
        int index);
  }
  /**
   * Protobuf type {@code com.palantir.atlasdb.protos.generated.SchemaVersions}
   */
  public static final class SchemaVersions extends
      com.google.protobuf.GeneratedMessage implements
      // @@protoc_insertion_point(message_implements:com.palantir.atlasdb.protos.generated.SchemaVersions)
      SchemaVersionsOrBuilder {
    // Use SchemaVersions.newBuilder() to construct.
    private SchemaVersions(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private SchemaVersions(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final SchemaVersions defaultInstance;
    public static SchemaVersions getDefaultInstance() {
      return defaultInstance;
    }

    public SchemaVersions getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private SchemaVersions(
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
            case 10: {
              if (!((mutable_bitField0_ & 0x00000001) == 0x00000001)) {
                versions_ = new java.util.ArrayList<com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion>();
                mutable_bitField0_ |= 0x00000001;
              }
              versions_.add(input.readMessage(com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion.PARSER, extensionRegistry));
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
        if (((mutable_bitField0_ & 0x00000001) == 0x00000001)) {
          versions_ = java.util.Collections.unmodifiableList(versions_);
        }
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.palantir.atlasdb.protos.generated.UpgradePersistence.internal_static_com_palantir_atlasdb_protos_generated_SchemaVersions_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.palantir.atlasdb.protos.generated.UpgradePersistence.internal_static_com_palantir_atlasdb_protos_generated_SchemaVersions_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions.class, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions.Builder.class);
    }

    public static com.google.protobuf.Parser<SchemaVersions> PARSER =
        new com.google.protobuf.AbstractParser<SchemaVersions>() {
      public SchemaVersions parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new SchemaVersions(input, extensionRegistry);
      }
    };

    @java.lang.Override
    public com.google.protobuf.Parser<SchemaVersions> getParserForType() {
      return PARSER;
    }

    public static final int VERSIONS_FIELD_NUMBER = 1;
    private java.util.List<com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion> versions_;
    /**
     * <code>repeated .com.palantir.atlasdb.protos.generated.SchemaVersion versions = 1;</code>
     */
    public java.util.List<com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion> getVersionsList() {
      return versions_;
    }
    /**
     * <code>repeated .com.palantir.atlasdb.protos.generated.SchemaVersion versions = 1;</code>
     */
    public java.util.List<? extends com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersionOrBuilder> 
        getVersionsOrBuilderList() {
      return versions_;
    }
    /**
     * <code>repeated .com.palantir.atlasdb.protos.generated.SchemaVersion versions = 1;</code>
     */
    public int getVersionsCount() {
      return versions_.size();
    }
    /**
     * <code>repeated .com.palantir.atlasdb.protos.generated.SchemaVersion versions = 1;</code>
     */
    public com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion getVersions(int index) {
      return versions_.get(index);
    }
    /**
     * <code>repeated .com.palantir.atlasdb.protos.generated.SchemaVersion versions = 1;</code>
     */
    public com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersionOrBuilder getVersionsOrBuilder(
        int index) {
      return versions_.get(index);
    }

    private void initFields() {
      versions_ = java.util.Collections.emptyList();
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      for (int i = 0; i < getVersionsCount(); i++) {
        if (!getVersions(i).isInitialized()) {
          memoizedIsInitialized = 0;
          return false;
        }
      }
      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      for (int i = 0; i < versions_.size(); i++) {
        output.writeMessage(1, versions_.get(i));
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      for (int i = 0; i < versions_.size(); i++) {
        size += com.google.protobuf.CodedOutputStream
          .computeMessageSize(1, versions_.get(i));
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

    public static com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions prototype) {
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
     * Protobuf type {@code com.palantir.atlasdb.protos.generated.SchemaVersions}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:com.palantir.atlasdb.protos.generated.SchemaVersions)
        com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersionsOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return com.palantir.atlasdb.protos.generated.UpgradePersistence.internal_static_com_palantir_atlasdb_protos_generated_SchemaVersions_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return com.palantir.atlasdb.protos.generated.UpgradePersistence.internal_static_com_palantir_atlasdb_protos_generated_SchemaVersions_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions.class, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions.Builder.class);
      }

      // Construct using com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions.newBuilder()
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
          getVersionsFieldBuilder();
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        if (versionsBuilder_ == null) {
          versions_ = java.util.Collections.emptyList();
          bitField0_ = (bitField0_ & ~0x00000001);
        } else {
          versionsBuilder_.clear();
        }
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return com.palantir.atlasdb.protos.generated.UpgradePersistence.internal_static_com_palantir_atlasdb_protos_generated_SchemaVersions_descriptor;
      }

      public com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions getDefaultInstanceForType() {
        return com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions.getDefaultInstance();
      }

      public com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions build() {
        com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions buildPartial() {
        com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions result = new com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions(this);
        int from_bitField0_ = bitField0_;
        if (versionsBuilder_ == null) {
          if (((bitField0_ & 0x00000001) == 0x00000001)) {
            versions_ = java.util.Collections.unmodifiableList(versions_);
            bitField0_ = (bitField0_ & ~0x00000001);
          }
          result.versions_ = versions_;
        } else {
          result.versions_ = versionsBuilder_.build();
        }
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions) {
          return mergeFrom((com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions other) {
        if (other == com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions.getDefaultInstance()) return this;
        if (versionsBuilder_ == null) {
          if (!other.versions_.isEmpty()) {
            if (versions_.isEmpty()) {
              versions_ = other.versions_;
              bitField0_ = (bitField0_ & ~0x00000001);
            } else {
              ensureVersionsIsMutable();
              versions_.addAll(other.versions_);
            }
            onChanged();
          }
        } else {
          if (!other.versions_.isEmpty()) {
            if (versionsBuilder_.isEmpty()) {
              versionsBuilder_.dispose();
              versionsBuilder_ = null;
              versions_ = other.versions_;
              bitField0_ = (bitField0_ & ~0x00000001);
              versionsBuilder_ = 
                com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders ?
                   getVersionsFieldBuilder() : null;
            } else {
              versionsBuilder_.addAllMessages(other.versions_);
            }
          }
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        for (int i = 0; i < getVersionsCount(); i++) {
          if (!getVersions(i).isInitialized()) {
            
            return false;
          }
        }
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersions) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      private java.util.List<com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion> versions_ =
        java.util.Collections.emptyList();
      private void ensureVersionsIsMutable() {
        if (!((bitField0_ & 0x00000001) == 0x00000001)) {
          versions_ = new java.util.ArrayList<com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion>(versions_);
          bitField0_ |= 0x00000001;
         }
      }

      private com.google.protobuf.RepeatedFieldBuilder<
          com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion.Builder, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersionOrBuilder> versionsBuilder_;

      /**
       * <code>repeated .com.palantir.atlasdb.protos.generated.SchemaVersion versions = 1;</code>
       */
      public java.util.List<com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion> getVersionsList() {
        if (versionsBuilder_ == null) {
          return java.util.Collections.unmodifiableList(versions_);
        } else {
          return versionsBuilder_.getMessageList();
        }
      }
      /**
       * <code>repeated .com.palantir.atlasdb.protos.generated.SchemaVersion versions = 1;</code>
       */
      public int getVersionsCount() {
        if (versionsBuilder_ == null) {
          return versions_.size();
        } else {
          return versionsBuilder_.getCount();
        }
      }
      /**
       * <code>repeated .com.palantir.atlasdb.protos.generated.SchemaVersion versions = 1;</code>
       */
      public com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion getVersions(int index) {
        if (versionsBuilder_ == null) {
          return versions_.get(index);
        } else {
          return versionsBuilder_.getMessage(index);
        }
      }
      /**
       * <code>repeated .com.palantir.atlasdb.protos.generated.SchemaVersion versions = 1;</code>
       */
      public Builder setVersions(
          int index, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion value) {
        if (versionsBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureVersionsIsMutable();
          versions_.set(index, value);
          onChanged();
        } else {
          versionsBuilder_.setMessage(index, value);
        }
        return this;
      }
      /**
       * <code>repeated .com.palantir.atlasdb.protos.generated.SchemaVersion versions = 1;</code>
       */
      public Builder setVersions(
          int index, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion.Builder builderForValue) {
        if (versionsBuilder_ == null) {
          ensureVersionsIsMutable();
          versions_.set(index, builderForValue.build());
          onChanged();
        } else {
          versionsBuilder_.setMessage(index, builderForValue.build());
        }
        return this;
      }
      /**
       * <code>repeated .com.palantir.atlasdb.protos.generated.SchemaVersion versions = 1;</code>
       */
      public Builder addVersions(com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion value) {
        if (versionsBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureVersionsIsMutable();
          versions_.add(value);
          onChanged();
        } else {
          versionsBuilder_.addMessage(value);
        }
        return this;
      }
      /**
       * <code>repeated .com.palantir.atlasdb.protos.generated.SchemaVersion versions = 1;</code>
       */
      public Builder addVersions(
          int index, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion value) {
        if (versionsBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureVersionsIsMutable();
          versions_.add(index, value);
          onChanged();
        } else {
          versionsBuilder_.addMessage(index, value);
        }
        return this;
      }
      /**
       * <code>repeated .com.palantir.atlasdb.protos.generated.SchemaVersion versions = 1;</code>
       */
      public Builder addVersions(
          com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion.Builder builderForValue) {
        if (versionsBuilder_ == null) {
          ensureVersionsIsMutable();
          versions_.add(builderForValue.build());
          onChanged();
        } else {
          versionsBuilder_.addMessage(builderForValue.build());
        }
        return this;
      }
      /**
       * <code>repeated .com.palantir.atlasdb.protos.generated.SchemaVersion versions = 1;</code>
       */
      public Builder addVersions(
          int index, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion.Builder builderForValue) {
        if (versionsBuilder_ == null) {
          ensureVersionsIsMutable();
          versions_.add(index, builderForValue.build());
          onChanged();
        } else {
          versionsBuilder_.addMessage(index, builderForValue.build());
        }
        return this;
      }
      /**
       * <code>repeated .com.palantir.atlasdb.protos.generated.SchemaVersion versions = 1;</code>
       */
      public Builder addAllVersions(
          java.lang.Iterable<? extends com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion> values) {
        if (versionsBuilder_ == null) {
          ensureVersionsIsMutable();
          com.google.protobuf.AbstractMessageLite.Builder.addAll(
              values, versions_);
          onChanged();
        } else {
          versionsBuilder_.addAllMessages(values);
        }
        return this;
      }
      /**
       * <code>repeated .com.palantir.atlasdb.protos.generated.SchemaVersion versions = 1;</code>
       */
      public Builder clearVersions() {
        if (versionsBuilder_ == null) {
          versions_ = java.util.Collections.emptyList();
          bitField0_ = (bitField0_ & ~0x00000001);
          onChanged();
        } else {
          versionsBuilder_.clear();
        }
        return this;
      }
      /**
       * <code>repeated .com.palantir.atlasdb.protos.generated.SchemaVersion versions = 1;</code>
       */
      public Builder removeVersions(int index) {
        if (versionsBuilder_ == null) {
          ensureVersionsIsMutable();
          versions_.remove(index);
          onChanged();
        } else {
          versionsBuilder_.remove(index);
        }
        return this;
      }
      /**
       * <code>repeated .com.palantir.atlasdb.protos.generated.SchemaVersion versions = 1;</code>
       */
      public com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion.Builder getVersionsBuilder(
          int index) {
        return getVersionsFieldBuilder().getBuilder(index);
      }
      /**
       * <code>repeated .com.palantir.atlasdb.protos.generated.SchemaVersion versions = 1;</code>
       */
      public com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersionOrBuilder getVersionsOrBuilder(
          int index) {
        if (versionsBuilder_ == null) {
          return versions_.get(index);  } else {
          return versionsBuilder_.getMessageOrBuilder(index);
        }
      }
      /**
       * <code>repeated .com.palantir.atlasdb.protos.generated.SchemaVersion versions = 1;</code>
       */
      public java.util.List<? extends com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersionOrBuilder> 
           getVersionsOrBuilderList() {
        if (versionsBuilder_ != null) {
          return versionsBuilder_.getMessageOrBuilderList();
        } else {
          return java.util.Collections.unmodifiableList(versions_);
        }
      }
      /**
       * <code>repeated .com.palantir.atlasdb.protos.generated.SchemaVersion versions = 1;</code>
       */
      public com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion.Builder addVersionsBuilder() {
        return getVersionsFieldBuilder().addBuilder(
            com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion.getDefaultInstance());
      }
      /**
       * <code>repeated .com.palantir.atlasdb.protos.generated.SchemaVersion versions = 1;</code>
       */
      public com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion.Builder addVersionsBuilder(
          int index) {
        return getVersionsFieldBuilder().addBuilder(
            index, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion.getDefaultInstance());
      }
      /**
       * <code>repeated .com.palantir.atlasdb.protos.generated.SchemaVersion versions = 1;</code>
       */
      public java.util.List<com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion.Builder> 
           getVersionsBuilderList() {
        return getVersionsFieldBuilder().getBuilderList();
      }
      private com.google.protobuf.RepeatedFieldBuilder<
          com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion.Builder, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersionOrBuilder> 
          getVersionsFieldBuilder() {
        if (versionsBuilder_ == null) {
          versionsBuilder_ = new com.google.protobuf.RepeatedFieldBuilder<
              com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersion.Builder, com.palantir.atlasdb.protos.generated.UpgradePersistence.SchemaVersionOrBuilder>(
                  versions_,
                  ((bitField0_ & 0x00000001) == 0x00000001),
                  getParentForChildren(),
                  isClean());
          versions_ = null;
        }
        return versionsBuilder_;
      }

      // @@protoc_insertion_point(builder_scope:com.palantir.atlasdb.protos.generated.SchemaVersions)
    }

    static {
      defaultInstance = new SchemaVersions(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:com.palantir.atlasdb.protos.generated.SchemaVersions)
  }

  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_com_palantir_atlasdb_protos_generated_SchemaVersion_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_com_palantir_atlasdb_protos_generated_SchemaVersion_fieldAccessorTable;
  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_com_palantir_atlasdb_protos_generated_SchemaVersions_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_com_palantir_atlasdb_protos_generated_SchemaVersions_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n4com/palantir/atlasdb/protos/UpgradePer" +
      "sistence.proto\022%com.palantir.atlasdb.pro" +
      "tos.generated\"G\n\rSchemaVersion\022\017\n\007versio" +
      "n\030\001 \002(\003\022\016\n\006hotfix\030\002 \001(\003\022\025\n\rhotfix_hotfix" +
      "\030\003 \001(\003\"X\n\016SchemaVersions\022F\n\010versions\030\001 \003" +
      "(\01324.com.palantir.atlasdb.protos.generat" +
      "ed.SchemaVersion"
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
    internal_static_com_palantir_atlasdb_protos_generated_SchemaVersion_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_com_palantir_atlasdb_protos_generated_SchemaVersion_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessage.FieldAccessorTable(
        internal_static_com_palantir_atlasdb_protos_generated_SchemaVersion_descriptor,
        new java.lang.String[] { "Version", "Hotfix", "HotfixHotfix", });
    internal_static_com_palantir_atlasdb_protos_generated_SchemaVersions_descriptor =
      getDescriptor().getMessageTypes().get(1);
    internal_static_com_palantir_atlasdb_protos_generated_SchemaVersions_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessage.FieldAccessorTable(
        internal_static_com_palantir_atlasdb_protos_generated_SchemaVersions_descriptor,
        new java.lang.String[] { "Versions", });
  }

  // @@protoc_insertion_point(outer_class_scope)
}
