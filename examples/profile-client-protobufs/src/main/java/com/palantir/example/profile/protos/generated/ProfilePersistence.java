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
package com.palantir.example.profile.protos.generated;

public final class ProfilePersistence {
  private ProfilePersistence() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
  }
  public interface UserProfileOrBuilder extends
      // @@protoc_insertion_point(interface_extends:com.palantir.example.profile.protos.generated.UserProfile)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>required string name = 1;</code>
     */
    boolean hasName();
    /**
     * <code>required string name = 1;</code>
     */
    java.lang.String getName();
    /**
     * <code>required string name = 1;</code>
     */
    com.google.protobuf.ByteString
        getNameBytes();

    /**
     * <code>required sint64 birthEpochDay = 2;</code>
     */
    boolean hasBirthEpochDay();
    /**
     * <code>required sint64 birthEpochDay = 2;</code>
     */
    long getBirthEpochDay();
  }
  /**
   * Protobuf type {@code com.palantir.example.profile.protos.generated.UserProfile}
   */
  public static final class UserProfile extends
      com.google.protobuf.GeneratedMessage implements
      // @@protoc_insertion_point(message_implements:com.palantir.example.profile.protos.generated.UserProfile)
      UserProfileOrBuilder {
    // Use UserProfile.newBuilder() to construct.
    private UserProfile(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private UserProfile(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final UserProfile defaultInstance;
    public static UserProfile getDefaultInstance() {
      return defaultInstance;
    }

    public UserProfile getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private UserProfile(
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
              com.google.protobuf.ByteString bs = input.readBytes();
              bitField0_ |= 0x00000001;
              name_ = bs;
              break;
            }
            case 16: {
              bitField0_ |= 0x00000002;
              birthEpochDay_ = input.readSInt64();
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
      return com.palantir.example.profile.protos.generated.ProfilePersistence.internal_static_com_palantir_example_profile_protos_generated_UserProfile_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.palantir.example.profile.protos.generated.ProfilePersistence.internal_static_com_palantir_example_profile_protos_generated_UserProfile_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile.class, com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile.Builder.class);
    }

    public static com.google.protobuf.Parser<UserProfile> PARSER =
        new com.google.protobuf.AbstractParser<UserProfile>() {
      public UserProfile parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new UserProfile(input, extensionRegistry);
      }
    };

    @java.lang.Override
    public com.google.protobuf.Parser<UserProfile> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    public static final int NAME_FIELD_NUMBER = 1;
    private java.lang.Object name_;
    /**
     * <code>required string name = 1;</code>
     */
    public boolean hasName() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>required string name = 1;</code>
     */
    public java.lang.String getName() {
      java.lang.Object ref = name_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          name_ = s;
        }
        return s;
      }
    }
    /**
     * <code>required string name = 1;</code>
     */
    public com.google.protobuf.ByteString
        getNameBytes() {
      java.lang.Object ref = name_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        name_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    public static final int BIRTHEPOCHDAY_FIELD_NUMBER = 2;
    private long birthEpochDay_;
    /**
     * <code>required sint64 birthEpochDay = 2;</code>
     */
    public boolean hasBirthEpochDay() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>required sint64 birthEpochDay = 2;</code>
     */
    public long getBirthEpochDay() {
      return birthEpochDay_;
    }

    private void initFields() {
      name_ = "";
      birthEpochDay_ = 0L;
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      if (!hasName()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (!hasBirthEpochDay()) {
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
        output.writeBytes(1, getNameBytes());
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        output.writeSInt64(2, birthEpochDay_);
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
          .computeBytesSize(1, getNameBytes());
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        size += com.google.protobuf.CodedOutputStream
          .computeSInt64Size(2, birthEpochDay_);
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

    public static com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile prototype) {
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
     * Protobuf type {@code com.palantir.example.profile.protos.generated.UserProfile}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:com.palantir.example.profile.protos.generated.UserProfile)
        com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfileOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return com.palantir.example.profile.protos.generated.ProfilePersistence.internal_static_com_palantir_example_profile_protos_generated_UserProfile_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return com.palantir.example.profile.protos.generated.ProfilePersistence.internal_static_com_palantir_example_profile_protos_generated_UserProfile_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile.class, com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile.Builder.class);
      }

      // Construct using com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile.newBuilder()
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
        name_ = "";
        bitField0_ = (bitField0_ & ~0x00000001);
        birthEpochDay_ = 0L;
        bitField0_ = (bitField0_ & ~0x00000002);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return com.palantir.example.profile.protos.generated.ProfilePersistence.internal_static_com_palantir_example_profile_protos_generated_UserProfile_descriptor;
      }

      public com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile getDefaultInstanceForType() {
        return com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile.getDefaultInstance();
      }

      public com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile build() {
        com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile buildPartial() {
        com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile result = new com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.name_ = name_;
        if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
          to_bitField0_ |= 0x00000002;
        }
        result.birthEpochDay_ = birthEpochDay_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile) {
          return mergeFrom((com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile other) {
        if (other == com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile.getDefaultInstance()) return this;
        if (other.hasName()) {
          bitField0_ |= 0x00000001;
          name_ = other.name_;
          onChanged();
        }
        if (other.hasBirthEpochDay()) {
          setBirthEpochDay(other.getBirthEpochDay());
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        if (!hasName()) {
          
          return false;
        }
        if (!hasBirthEpochDay()) {
          
          return false;
        }
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      private java.lang.Object name_ = "";
      /**
       * <code>required string name = 1;</code>
       */
      public boolean hasName() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>required string name = 1;</code>
       */
      public java.lang.String getName() {
        java.lang.Object ref = name_;
        if (!(ref instanceof java.lang.String)) {
          com.google.protobuf.ByteString bs =
              (com.google.protobuf.ByteString) ref;
          java.lang.String s = bs.toStringUtf8();
          if (bs.isValidUtf8()) {
            name_ = s;
          }
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>required string name = 1;</code>
       */
      public com.google.protobuf.ByteString
          getNameBytes() {
        java.lang.Object ref = name_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          name_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>required string name = 1;</code>
       */
      public Builder setName(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        name_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required string name = 1;</code>
       */
      public Builder clearName() {
        bitField0_ = (bitField0_ & ~0x00000001);
        name_ = getDefaultInstance().getName();
        onChanged();
        return this;
      }
      /**
       * <code>required string name = 1;</code>
       */
      public Builder setNameBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        name_ = value;
        onChanged();
        return this;
      }

      private long birthEpochDay_ ;
      /**
       * <code>required sint64 birthEpochDay = 2;</code>
       */
      public boolean hasBirthEpochDay() {
        return ((bitField0_ & 0x00000002) == 0x00000002);
      }
      /**
       * <code>required sint64 birthEpochDay = 2;</code>
       */
      public long getBirthEpochDay() {
        return birthEpochDay_;
      }
      /**
       * <code>required sint64 birthEpochDay = 2;</code>
       */
      public Builder setBirthEpochDay(long value) {
        bitField0_ |= 0x00000002;
        birthEpochDay_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required sint64 birthEpochDay = 2;</code>
       */
      public Builder clearBirthEpochDay() {
        bitField0_ = (bitField0_ & ~0x00000002);
        birthEpochDay_ = 0L;
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:com.palantir.example.profile.protos.generated.UserProfile)
    }

    static {
      defaultInstance = new UserProfile(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:com.palantir.example.profile.protos.generated.UserProfile)
  }

  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_com_palantir_example_profile_protos_generated_UserProfile_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_com_palantir_example_profile_protos_generated_UserProfile_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n<com/palantir/example/profile/protos/Pr" +
      "ofilePersistence.proto\022-com.palantir.exa" +
      "mple.profile.protos.generated\"2\n\013UserPro" +
      "file\022\014\n\004name\030\001 \002(\t\022\025\n\rbirthEpochDay\030\002 \002(" +
      "\022"
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
    internal_static_com_palantir_example_profile_protos_generated_UserProfile_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_com_palantir_example_profile_protos_generated_UserProfile_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessage.FieldAccessorTable(
        internal_static_com_palantir_example_profile_protos_generated_UserProfile_descriptor,
        new java.lang.String[] { "Name", "BirthEpochDay", });
  }

  // @@protoc_insertion_point(outer_class_scope)
}
