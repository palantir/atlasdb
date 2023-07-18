/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.watch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockToken;
import com.palantir.logsafe.Unsafe;
import com.palantir.util.IndexEncodingUtils;
import com.palantir.util.IndexEncodingUtils.ChecksumType;
import com.palantir.util.IndexEncodingUtils.IndexEncodingResult;
import com.palantir.util.IndexEncodingUtils.KeyListChecksum;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.immutables.value.Value;

/*
 * Warning: This class contains custom serialization logic. Changing anything about the serialized format of this class
 * requires careful modification of the @JsonCreator and @JsonValue-annotated methods as well as the Wire-... classes
 * All serialization changes should be properly tested.
 */
@Unsafe
@Value.Immutable
@JsonTypeName(LockEvent.TYPE)
public abstract class LockEvent implements LockWatchEvent {
    static final String TYPE = "lock";
    private static final ChecksumType DEFAULT_CHECKSUM_TYPE = ChecksumType.CRC32_OF_DETERMINISTIC_HASHCODE;

    public abstract Set<LockDescriptor> lockDescriptors();

    public abstract LockToken lockToken();

    public abstract Optional<LockRequestMetadata> metadata();

    @Override
    public int size() {
        return lockDescriptors().size();
    }

    @Override
    public <T> T accept(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    @JsonCreator
    public static LockEvent create(
            @JsonProperty("sequence") long sequence,
            @JsonProperty("lockDescriptors") List<LockDescriptor> lockDescriptors,
            @JsonProperty("lockToken") LockToken lockToken,
            @JsonProperty("metadata") Optional<WireLockRequestMetadata> optWireMetadata) {
        Optional<LockRequestMetadata> metadata =
                optWireMetadata.map(wireMetadata -> convertFromWireMetadata(lockDescriptors, wireMetadata));
        return ImmutableLockEvent.builder()
                .sequence(sequence)
                .lockDescriptors(ImmutableSet.copyOf(lockDescriptors))
                .lockToken(lockToken)
                .metadata(metadata)
                .build();
    }

    @Unsafe
    @JsonValue
    public WireLockEvent toWireLockEvent() {
        Optional<IndexEncodingResult<LockDescriptor, ChangeMetadata>> optIndexEncodingResult = metadata()
                .map(metadata -> IndexEncodingUtils.encode(
                        lockDescriptors(), metadata.lockDescriptorToChangeMetadata(), DEFAULT_CHECKSUM_TYPE));
        // If the lock event has metadata, we need to use the key list that was used to encode the metadata.
        // Otherwise, any ordering of the lock descriptors is fine.
        List<LockDescriptor> orderedLocks = optIndexEncodingResult
                .map(IndexEncodingResult::keyList)
                .orElseGet(() -> ImmutableList.copyOf(lockDescriptors()));
        Optional<WireLockRequestMetadata> wireMetadata =
                optIndexEncodingResult.map(LockEvent::buildWireLockRequestMetadata);
        return WireLockEvent.builder()
                .sequence(sequence())
                .lockDescriptors(orderedLocks)
                .lockToken(lockToken())
                .metadata(wireMetadata)
                .build();
    }

    public static LockWatchEvent.Builder builder(Set<LockDescriptor> lockDescriptors, LockToken lockToken) {
        return builder(lockDescriptors, lockToken, Optional.empty());
    }

    public static LockWatchEvent.Builder builder(
            Set<LockDescriptor> lockDescriptors, LockToken lockToken, Optional<LockRequestMetadata> metadata) {
        ImmutableLockEvent.Builder builder = ImmutableLockEvent.builder()
                .lockDescriptors(lockDescriptors)
                .lockToken(lockToken)
                .metadata(metadata);
        return seq -> builder.sequence(seq).build();
    }

    private static WireLockRequestMetadata buildWireLockRequestMetadata(
            IndexEncodingResult<LockDescriptor, ChangeMetadata> result) {
        return WireLockRequestMetadata.builder()
                .indexToChangeMetadata(result.indexToValue())
                .lockListChecksum(LockListChecksum.of(
                        result.keyListChecksum().type().getId(),
                        result.keyListChecksum().value()))
                .build();
    }

    private static LockRequestMetadata convertFromWireMetadata(
            List<LockDescriptor> lockDescriptors, WireLockRequestMetadata wireMetadata) {
        KeyListChecksum checksum = KeyListChecksum.of(
                ChecksumType.valueOf(wireMetadata.lockListChecksum().typeId()),
                wireMetadata.lockListChecksum().value());
        IndexEncodingResult<LockDescriptor, ChangeMetadata> indexEncodingResult =
                IndexEncodingResult.<LockDescriptor, ChangeMetadata>builder()
                        .keyList(lockDescriptors)
                        .indexToValue(wireMetadata.indexToChangeMetadata())
                        .keyListChecksum(checksum)
                        .build();
        return LockRequestMetadata.of(IndexEncodingUtils.decode(indexEncodingResult));
    }

    // This class is explicitly missing @JsonDeserialize as it should only be used for serialization.
    // Deserialization should take place in the @JsonCreator-annotated static constructor method.
    @Unsafe
    @Value.Immutable
    @JsonSerialize(as = ImmutableWireLockEvent.class)
    @JsonTypeName(LockEvent.TYPE)
    public interface WireLockEvent {
        long sequence();

        List<LockDescriptor> lockDescriptors();

        LockToken lockToken();

        // If metadata is absent, it should be completely omitted
        @JsonInclude(Include.NON_ABSENT)
        Optional<WireLockRequestMetadata> metadata();

        static ImmutableWireLockEvent.Builder builder() {
            return ImmutableWireLockEvent.builder();
        }
    }

    @Unsafe
    @Value.Immutable
    @JsonSerialize(as = ImmutableWireLockRequestMetadata.class)
    @JsonDeserialize(as = ImmutableWireLockRequestMetadata.class)
    public interface WireLockRequestMetadata {

        Map<Integer, ChangeMetadata> indexToChangeMetadata();

        LockListChecksum lockListChecksum();

        static ImmutableWireLockRequestMetadata.Builder builder() {
            return ImmutableWireLockRequestMetadata.builder();
        }
    }

    @Value.Immutable
    @JsonSerialize(as = ImmutableLockListChecksum.class)
    @JsonDeserialize(as = ImmutableLockListChecksum.class)
    public interface LockListChecksum {

        @Value.Parameter
        int typeId();

        @Value.Parameter
        byte[] value();

        static LockListChecksum of(int typeId, byte[] value) {
            return ImmutableLockListChecksum.of(typeId, value);
        }
    }
}
