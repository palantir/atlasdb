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

package com.palantir.lock.watch;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.timelock.api.ConjureChangeMetadata;
import com.palantir.atlasdb.timelock.api.ConjureCreatedChangeMetadata;
import com.palantir.atlasdb.timelock.api.ConjureDeletedChangeMetadata;
import com.palantir.atlasdb.timelock.api.ConjureLockDescriptorListChecksum;
import com.palantir.atlasdb.timelock.api.ConjureLockRequestMetadata;
import com.palantir.atlasdb.timelock.api.ConjureUnchangedChangeMetadata;
import com.palantir.atlasdb.timelock.api.ConjureUpdatedChangeMetadata;
import com.palantir.common.streams.KeyedStream;
import com.palantir.conjure.java.lib.Bytes;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.watch.ChangeMetadata.Created;
import com.palantir.lock.watch.ChangeMetadata.Deleted;
import com.palantir.lock.watch.ChangeMetadata.Unchanged;
import com.palantir.lock.watch.ChangeMetadata.Updated;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.Unsafe;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.util.IndexEncodingUtils;
import com.palantir.util.IndexEncodingUtils.ChecksumType;
import com.palantir.util.IndexEncodingUtils.IndexEncodingResult;
import com.palantir.util.IndexEncodingUtils.KeyListChecksum;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.immutables.value.Value;

public final class ConjureLockRequestMetadataUtils {
    private ConjureLockRequestMetadataUtils() {}

    @VisibleForTesting
    static final ChecksumType DEFAULT_CHECKSUM_TYPE = ChecksumType.CRC32_OF_DETERMINISTIC_HASHCODE;

    private static final SafeLogger log = SafeLoggerFactory.get(ConjureLockRequestMetadataUtils.class);

    public static ConjureMetadataConversionResult toConjureIndexEncoded(
            Set<LockDescriptor> lockDescriptors, LockRequestMetadata metadata) {
        IndexEncodingResult<LockDescriptor, ConjureChangeMetadata> encoded = IndexEncodingUtils.encode(
                lockDescriptors,
                metadata.lockDescriptorToChangeMetadata(),
                changeMetadata -> changeMetadata.accept(ChangeMetadataToConjureVisitor.INSTANCE),
                DEFAULT_CHECKSUM_TYPE);
        KeyListChecksum checksum = encoded.keyListChecksum();
        ConjureLockRequestMetadata conjureLockRequestMetadata = ConjureLockRequestMetadata.builder()
                .indexToChangeMetadata(encoded.indexToValue())
                .lockListChecksum(checksumToConjure(checksum))
                .build();
        return ImmutableConjureMetadataConversionResult.builder()
                .lockList(encoded.keyList())
                .conjureMetadata(conjureLockRequestMetadata)
                .build();
    }

    public static LockRequestMetadata fromConjureIndexEncoded(ConjureMetadataConversionResult conversionResult) {
        List<LockDescriptor> keyList = conversionResult.lockList();
        ConjureLockRequestMetadata conjureMetadata = conversionResult.conjureMetadata();
        KeyListChecksum checksum =
                checksumFromConjure(conversionResult.conjureMetadata().getLockListChecksum());
        IndexEncodingResult<LockDescriptor, ConjureChangeMetadata> encoded =
                IndexEncodingResult.<LockDescriptor, ConjureChangeMetadata>builder()
                        .keyList(keyList)
                        .indexToValue(conjureMetadata.getIndexToChangeMetadata())
                        .keyListChecksum(checksum)
                        .build();
        Map<LockDescriptor, Optional<ChangeMetadata>> optChangeMetadata = IndexEncodingUtils.decode(
                encoded,
                conjureChangeMetadata -> conjureChangeMetadata.accept(ChangeMetadataFromConjureVisitor.INSTANCE));
        Map<LockDescriptor, ChangeMetadata> changeMetadata = KeyedStream.ofEntries(
                        optChangeMetadata.entrySet().stream())
                .flatMap(Optional::stream)
                .collectToMap();
        return LockRequestMetadata.of(changeMetadata);
    }

    private static ConjureLockDescriptorListChecksum checksumToConjure(KeyListChecksum checksum) {
        return ConjureLockDescriptorListChecksum.of(checksum.type().getId(), Bytes.from(checksum.value()));
    }

    private static KeyListChecksum checksumFromConjure(ConjureLockDescriptorListChecksum conjureChecksum) {
        return KeyListChecksum.of(
                ChecksumType.valueOf(conjureChecksum.getTypeId()),
                conjureChecksum.getValue().asNewByteArray());
    }

    @Unsafe
    @Value.Immutable
    public interface ConjureMetadataConversionResult {
        List<LockDescriptor> lockList();

        ConjureLockRequestMetadata conjureMetadata();

        static ImmutableConjureMetadataConversionResult.Builder builder() {
            return ImmutableConjureMetadataConversionResult.builder();
        }
    }

    private enum ChangeMetadataToConjureVisitor implements ChangeMetadata.Visitor<ConjureChangeMetadata> {
        INSTANCE;

        @Override
        public ConjureChangeMetadata visit(Unchanged unchanged) {
            return ConjureChangeMetadata.unchanged(ConjureUnchangedChangeMetadata.of());
        }

        @Override
        public ConjureChangeMetadata visit(Updated updated) {
            return ConjureChangeMetadata.updated(
                    ConjureUpdatedChangeMetadata.of(Bytes.from(updated.oldValue()), Bytes.from(updated.newValue())));
        }

        @Override
        public ConjureChangeMetadata visit(Deleted deleted) {
            return ConjureChangeMetadata.deleted(ConjureDeletedChangeMetadata.of(Bytes.from(deleted.oldValue())));
        }

        @Override
        public ConjureChangeMetadata visit(Created created) {
            return ConjureChangeMetadata.created(ConjureCreatedChangeMetadata.of(Bytes.from(created.newValue())));
        }
    }

    private enum ChangeMetadataFromConjureVisitor implements ConjureChangeMetadata.Visitor<Optional<ChangeMetadata>> {
        INSTANCE;

        @Override
        public Optional<ChangeMetadata> visitUnchanged(ConjureUnchangedChangeMetadata unchanged) {
            return Optional.of(ChangeMetadata.unchanged());
        }

        @Override
        public Optional<ChangeMetadata> visitUpdated(ConjureUpdatedChangeMetadata updated) {
            return Optional.of(ChangeMetadata.updated(
                    updated.getOldValue().asNewByteArray(),
                    updated.getNewValue().asNewByteArray()));
        }

        @Override
        public Optional<ChangeMetadata> visitDeleted(ConjureDeletedChangeMetadata deleted) {
            return Optional.of(ChangeMetadata.deleted(deleted.getOldValue().asNewByteArray()));
        }

        @Override
        public Optional<ChangeMetadata> visitCreated(ConjureCreatedChangeMetadata created) {
            return Optional.of(ChangeMetadata.created(created.getNewValue().asNewByteArray()));
        }

        @Override
        public Optional<ChangeMetadata> visitUnknown(String unknownType) {
            log.trace(
                    "Encountered an unknown ConjureChangeMetadata type. This is likely a new type that was added in a"
                            + " future version. This ChangeMetadata will be discarded",
                    SafeArg.of("unknownType", unknownType));
            return Optional.empty();
        }
    }
}
