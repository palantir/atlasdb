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
import com.palantir.atlasdb.timelock.api.ConjureLockRequestMetadata;
import com.palantir.atlasdb.timelock.api.ConjureUnchangedChangeMetadata;
import com.palantir.atlasdb.timelock.api.ConjureUpdatedChangeMetadata;
import com.palantir.conjure.java.lib.Bytes;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.watch.ChangeMetadata.Created;
import com.palantir.lock.watch.ChangeMetadata.Deleted;
import com.palantir.lock.watch.ChangeMetadata.Unchanged;
import com.palantir.lock.watch.ChangeMetadata.Updated;
import com.palantir.logsafe.Unsafe;
import com.palantir.util.IndexEncodingUtils;
import com.palantir.util.IndexEncodingUtils.ChecksumType;
import com.palantir.util.IndexEncodingUtils.IndexEncodingResult;
import com.palantir.util.IndexEncodingUtils.KeyListChecksum;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.immutables.value.Value;

public final class ConjureLockRequestMetadataUtils {
    private ConjureLockRequestMetadataUtils() {}

    @VisibleForTesting
    static final ChecksumType DEFAULT_CHECKSUM_TYPE = ChecksumType.CRC32_OF_DETERMINISTIC_HASHCODE;

    private static final ChangeMetadataToConjureVisitor TO_CONJURE_VISITOR = new ChangeMetadataToConjureVisitor();
    private static final ChangeMetadataFromConjureVisitor FROM_CONJURE_VISITOR = new ChangeMetadataFromConjureVisitor();

    public static ConjureMetadataConversionResult toConjureIndexEncoded(
            Set<LockDescriptor> lockDescriptors, LockRequestMetadata metadata) {
        IndexEncodingResult<LockDescriptor, ConjureChangeMetadata> encoded = IndexEncodingUtils.encode(
                lockDescriptors,
                metadata.lockDescriptorToChangeMetadata(),
                changeMetadata -> changeMetadata.accept(TO_CONJURE_VISITOR),
                DEFAULT_CHECKSUM_TYPE);
        KeyListChecksum checksum = encoded.keyListChecksum();
        ConjureLockRequestMetadata conjureLockRequestMetadata = ConjureLockRequestMetadata.builder()
                .indexToChangeMetadata(encoded.indexToValue())
                .checksumTypeId(checksum.type().getId())
                .checksumValue(Bytes.from(checksum.value()))
                .build();
        return ImmutableConjureMetadataConversionResult.builder()
                .lockList(encoded.keyList())
                .conjureMetadata(conjureLockRequestMetadata)
                .build();
    }

    public static LockRequestMetadata fromConjureIndexEncoded(ConjureMetadataConversionResult conversionResult) {
        List<LockDescriptor> keyList = conversionResult.lockList();
        ConjureLockRequestMetadata conjureMetadata = conversionResult.conjureMetadata();
        ChecksumType checksumType = ChecksumType.valueOf(conjureMetadata.getChecksumTypeId());
        KeyListChecksum checksum = KeyListChecksum.of(
                checksumType, conjureMetadata.getChecksumValue().asNewByteArray());
        IndexEncodingResult<LockDescriptor, ConjureChangeMetadata> encoded =
                IndexEncodingResult.<LockDescriptor, ConjureChangeMetadata>builder()
                        .keyList(keyList)
                        .indexToValue(conjureMetadata.getIndexToChangeMetadata())
                        .keyListChecksum(checksum)
                        .build();
        Map<LockDescriptor, ChangeMetadata> changeMetadata = IndexEncodingUtils.decode(
                encoded, conjureChangeMetadata -> conjureChangeMetadata.accept(FROM_CONJURE_VISITOR));
        // visitUnknown() will return null
        changeMetadata.values().removeIf(Objects::isNull);
        return LockRequestMetadata.of(changeMetadata);
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

    private static final class ChangeMetadataToConjureVisitor implements ChangeMetadata.Visitor<ConjureChangeMetadata> {

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

    private static final class ChangeMetadataFromConjureVisitor
            implements ConjureChangeMetadata.Visitor<ChangeMetadata> {

        @Override
        public ChangeMetadata visitUnchanged(ConjureUnchangedChangeMetadata unchanged) {
            return ChangeMetadata.unchanged();
        }

        @Override
        public ChangeMetadata visitUpdated(ConjureUpdatedChangeMetadata updated) {
            return ChangeMetadata.updated(
                    updated.getOldValue().asNewByteArray(),
                    updated.getNewValue().asNewByteArray());
        }

        @Override
        public ChangeMetadata visitDeleted(ConjureDeletedChangeMetadata deleted) {
            return ChangeMetadata.deleted(deleted.getOldValue().asNewByteArray());
        }

        @Override
        public ChangeMetadata visitCreated(ConjureCreatedChangeMetadata created) {
            return ChangeMetadata.created(created.getNewValue().asNewByteArray());
        }

        @Override
        public ChangeMetadata visitUnknown(String unknownType) {
            // caller should handle this case
            return null;
        }
    }
}
