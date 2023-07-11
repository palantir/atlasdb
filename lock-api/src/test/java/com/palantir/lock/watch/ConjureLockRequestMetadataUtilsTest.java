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
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.encoding.PtBytes;
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
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.watch.ConjureLockRequestMetadataUtils.ConjureMetadataConversionResult;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.util.IndexEncodingUtils;
import com.palantir.util.IndexEncodingUtils.KeyListChecksum;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.BeforeClass;
import org.junit.Test;

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
    private static final List<ChangeMetadata> CHANGE_METADATA_LIST = ImmutableList.of(
            ChangeMetadata.unchanged(),
            ChangeMetadata.updated(BYTES_OLD, BYTES_NEW),
            ChangeMetadata.deleted(BYTES_DELETED),
            ChangeMetadata.created(BYTES_CREATED));
    // Although this is quite verbose, we explicitly want to test all possible types of change metadata and ensure
    // that we do the conversion right for each of them.
    private static final Map<LockDescriptor, ChangeMetadata> LOCKS_WITH_METADATA = ImmutableMap.of(
            LOCK_1,
            CHANGE_METADATA_LIST.get(0),
            LOCK_2,
            CHANGE_METADATA_LIST.get(1),
            LOCK_3,
            CHANGE_METADATA_LIST.get(2),
            LOCK_4,
            CHANGE_METADATA_LIST.get(3));
    private static final LockRequestMetadata LOCK_REQUEST_METADATA = LockRequestMetadata.of(LOCKS_WITH_METADATA);
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
    private static final Random RANDOM = new Random();
    private static ConjureMetadataConversionResult conjureMetadataConversionResult;

    // This is a good candidate for a static construction method, but we would rather avoid testing internals of
    // IndexEncodingUtils (checksum computation) within the tests for Conjure conversion.
    @BeforeClass
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
    public void convertingToAndFromConjureIsIdentityForRandomData() {
        Set<LockDescriptor> lockDescriptors = Stream.generate(UUID::randomUUID)
                .map(UUID::toString)
                .map(StringLockDescriptor::of)
                .limit(1000)
                .collect(Collectors.toSet());
        LockRequestMetadata metadata = createRandomLockRequestMetadataFor(lockDescriptors);
        assertThat(ConjureLockRequestMetadataUtils.fromConjureIndexEncoded(
                        ConjureLockRequestMetadataUtils.toConjureIndexEncoded(lockDescriptors, metadata)))
                .isEqualTo(metadata);
    }

    @Test
    public void changedLockOrderIsDetected() {
        ConjureMetadataConversionResult conversionResult = ImmutableConjureMetadataConversionResult.copyOf(
                        conjureMetadataConversionResult)
                .withLockList(LOCK_2, LOCK_1, LOCK_3, LOCK_4);
        assertThatLoggableExceptionThrownBy(
                        () -> ConjureLockRequestMetadataUtils.fromConjureIndexEncoded(conversionResult))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessageStartingWith("Key list integrity check failed");
    }

    @Test
    public void handlesEmptyMetadata() {
        assertThat(ConjureLockRequestMetadataUtils.fromConjureIndexEncoded(
                        ConjureLockRequestMetadataUtils.toConjureIndexEncoded(
                                ImmutableSet.of(), LockRequestMetadata.of(ImmutableMap.of()))))
                .isEqualTo(LockRequestMetadata.of(ImmutableMap.of()));
    }

    private static LockRequestMetadata createRandomLockRequestMetadataFor(Set<LockDescriptor> lockDescriptors) {
        List<ChangeMetadata> shuffled = new ArrayList<>(CHANGE_METADATA_LIST);
        Collections.shuffle(shuffled, RANDOM);
        Iterator<ChangeMetadata> iterator = Iterables.cycle(shuffled).iterator();
        Map<LockDescriptor, ChangeMetadata> lockDescriptorToChangeMetadata = KeyedStream.of(lockDescriptors.stream())
                .filter(_unused -> RANDOM.nextBoolean())
                .map(_unused -> iterator.next())
                .collectToMap();
        return LockRequestMetadata.of(lockDescriptorToChangeMetadata);
    }
}
