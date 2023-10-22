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

import static com.palantir.logsafe.testing.Assertions.assertThatLoggableExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.timelock.api.ConjureChangeMetadata;
import com.palantir.atlasdb.timelock.api.ConjureCreatedChangeMetadata;
import com.palantir.atlasdb.timelock.api.ConjureDeletedChangeMetadata;
import com.palantir.atlasdb.timelock.api.ConjureLockDescriptorListChecksum;
import com.palantir.atlasdb.timelock.api.ConjureLockRequestMetadata;
import com.palantir.atlasdb.timelock.api.ConjureUnchangedChangeMetadata;
import com.palantir.atlasdb.timelock.api.ConjureUpdatedChangeMetadata;
import com.palantir.conjure.java.lib.Bytes;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.watch.ConjureLockRequestMetadataUtils.ConjureMetadataConversionResult;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.util.IndexEncodingUtils;
import com.palantir.util.IndexEncodingUtils.KeyListChecksum;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ConjureLockRequestMetadataUtilsTest {
    private static final LockDescriptor LOCK_1 = StringLockDescriptor.of("lock1");
    private static final LockDescriptor LOCK_2 = StringLockDescriptor.of("lock2");
    private static final LockDescriptor LOCK_3 = StringLockDescriptor.of("lock3");
    private static final LockDescriptor LOCK_4 = StringLockDescriptor.of("lock4");
    private static final byte[] BYTES_OLD = PtBytes.toBytes("old");
    private static final byte[] BYTES_NEW = PtBytes.toBytes("new");
    private static final byte[] BYTES_DELETED = PtBytes.toBytes("deleted");
    private static final byte[] BYTES_CREATED = PtBytes.toBytes("created");
    private static final List<LockDescriptor> LOCK_LIST = ImmutableList.of(LOCK_1, LOCK_2, LOCK_3, LOCK_4);
    // Although this is quite verbose, we explicitly want to test all possible types of change metadata and ensure
    // that we do the conversion right for each of them.
    private static final LockRequestMetadata LOCK_REQUEST_METADATA = LockRequestMetadata.of(ImmutableMap.of(
            LOCK_1,
            ChangeMetadata.unchanged(),
            LOCK_2,
            ChangeMetadata.updated(BYTES_OLD, BYTES_NEW),
            LOCK_3,
            ChangeMetadata.deleted(BYTES_DELETED),
            LOCK_4,
            ChangeMetadata.created(BYTES_CREATED)));
    private static final Map<Integer, ConjureChangeMetadata> CONJURE_LOCKS_WITH_METADATA = ImmutableMap.of(
            0,
            ConjureChangeMetadata.unchanged(ConjureUnchangedChangeMetadata.of()),
            1,
            ConjureChangeMetadata.updated(
                    ConjureUpdatedChangeMetadata.of(Bytes.from(BYTES_OLD), Bytes.from(BYTES_NEW))),
            2,
            ConjureChangeMetadata.deleted(ConjureDeletedChangeMetadata.of(Bytes.from(BYTES_DELETED))),
            3,
            ConjureChangeMetadata.created(ConjureCreatedChangeMetadata.of(Bytes.from(BYTES_CREATED))));

    private static ConjureMetadataConversionResult conjureMetadataConversionResult;

    // This is a good candidate for a static construction method, but we would rather avoid testing internals of
    // IndexEncodingUtils (checksum computation) within the tests for Conjure conversion.
    @BeforeAll
    public static void setup() {
        KeyListChecksum checksum =
                IndexEncodingUtils.computeChecksum(ConjureLockRequestMetadataUtils.DEFAULT_CHECKSUM_TYPE, LOCK_LIST);
        ConjureLockRequestMetadata conjureMetadata = ConjureLockRequestMetadata.builder()
                .indexToChangeMetadata(CONJURE_LOCKS_WITH_METADATA)
                .lockListChecksum(
                        ConjureLockDescriptorListChecksum.of(checksum.type().getId(), Bytes.from(checksum.value())))
                .build();
        conjureMetadataConversionResult = ConjureMetadataConversionResult.builder()
                .lockList(LOCK_LIST)
                .conjureMetadata(conjureMetadata)
                .build();
    }

    @Test
    public void convertsToConjureCorrectly() {
        // ImmutableSet remembers insertion order, which is critical here
        assertThat(ConjureLockRequestMetadataUtils.toConjureIndexEncoded(
                        ImmutableSet.copyOf(LOCK_LIST), LOCK_REQUEST_METADATA))
                .isEqualTo(conjureMetadataConversionResult);
    }

    @Test
    public void convertsFromConjureCorrectly() {
        assertThat(ConjureLockRequestMetadataUtils.fromConjureIndexEncoded(conjureMetadataConversionResult))
                .isEqualTo(LOCK_REQUEST_METADATA);
    }

    @Test
    public void canConvertSparseMetadata() {
        List<LockDescriptor> lockDescriptors = IntStream.range(0, 10)
                .mapToObj(Integer::toString)
                .map(StringLockDescriptor::of)
                .collect(Collectors.toList());
        // Unique metadata on some locks, but not all
        Map<LockDescriptor, ChangeMetadata> lockDescriptorToChangeMetadata = ImmutableMap.of(
                lockDescriptors.get(0), ChangeMetadata.created(BYTES_CREATED),
                lockDescriptors.get(4), ChangeMetadata.deleted(BYTES_DELETED),
                lockDescriptors.get(9), ChangeMetadata.unchanged(),
                lockDescriptors.get(5), ChangeMetadata.updated(BYTES_OLD, BYTES_NEW),
                lockDescriptors.get(7), ChangeMetadata.unchanged());
        LockRequestMetadata metadata = LockRequestMetadata.of(lockDescriptorToChangeMetadata);

        assertThat(ConjureLockRequestMetadataUtils.fromConjureIndexEncoded(
                        ConjureLockRequestMetadataUtils.toConjureIndexEncoded(
                                ImmutableSet.copyOf(lockDescriptors), metadata)))
                .isEqualTo(metadata);
    }

    @Test
    public void changedLockOrderIsDetected() {
        List<LockDescriptor> modifiedLockList = new ArrayList<>(LOCK_LIST);
        Collections.swap(modifiedLockList, 0, 1);
        ConjureMetadataConversionResult conversionResult = ImmutableConjureMetadataConversionResult.copyOf(
                        conjureMetadataConversionResult)
                .withLockList(modifiedLockList);
        assertThatLoggableExceptionThrownBy(
                        () -> ConjureLockRequestMetadataUtils.fromConjureIndexEncoded(conversionResult))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessageStartingWith("Key list integrity check failed");
    }

    @Test
    public void handlesEmptyData() {
        assertThat(ConjureLockRequestMetadataUtils.fromConjureIndexEncoded(
                        ConjureLockRequestMetadataUtils.toConjureIndexEncoded(
                                ImmutableSet.of(), LockRequestMetadata.of(ImmutableMap.of()))))
                .isEqualTo(LockRequestMetadata.of(ImmutableMap.of()));
    }
}
