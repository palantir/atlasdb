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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.timelock.api.ConjureChangeMetadata;
import com.palantir.atlasdb.timelock.api.ConjureCreatedChangeMetadata;
import com.palantir.atlasdb.timelock.api.ConjureDeletedChangeMetadata;
import com.palantir.atlasdb.timelock.api.ConjureLockRequestMetadata;
import com.palantir.atlasdb.timelock.api.ConjureUnchangedChangeMetadata;
import com.palantir.atlasdb.timelock.api.ConjureUpdatedChangeMetadata;
import com.palantir.conjure.java.lib.Bytes;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.util.IndexEncodingUtils;
import com.palantir.util.IndexEncodingUtils.KeyListChecksum;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.BeforeClass;
import org.junit.Test;

public class ConjureLockRequestMetadataUtilsTest {
    private static final LockDescriptor LOCK_1 = StringLockDescriptor.of("lock1");
    private static final LockDescriptor LOCK_2 = StringLockDescriptor.of("lock2");
    private static final LockDescriptor LOCK_3 = StringLockDescriptor.of("lock3");
    private static final LockDescriptor LOCK_4 = StringLockDescriptor.of("lock4");

    private static final List<LockDescriptor> LOCK_LIST = ImmutableList.of(LOCK_1, LOCK_2, LOCK_3, LOCK_4);
    // LinkedHashSet remembers insertion order, which is important for the tests below.
    private static final Set<LockDescriptor> LOCK_SET = new LinkedHashSet<>(LOCK_LIST);

    // Although this is quite verbose, we explicitly want to test all possible types of change metadata and ensure
    // that we do the conversion right for each of them.
    private static final Map<LockDescriptor, ChangeMetadata> LOCKS_WITH_METADATA = ImmutableMap.of(
            LOCK_1,
            ChangeMetadata.unchanged(),
            LOCK_2,
            ChangeMetadata.updated(PtBytes.toBytes("old"), PtBytes.toBytes("new")),
            LOCK_3,
            ChangeMetadata.deleted(PtBytes.toBytes("deleted")),
            LOCK_4,
            ChangeMetadata.created(PtBytes.toBytes("created")));
    private static final LockRequestMetadata LOCK_REQUEST_METADATA = LockRequestMetadata.of(LOCKS_WITH_METADATA);

    private static final Map<Integer, ConjureChangeMetadata> CONJURE_LOCKS_WITH_METADATA = ImmutableMap.of(
            0,
            ConjureChangeMetadata.unchanged(ConjureUnchangedChangeMetadata.of()),
            1,
            ConjureChangeMetadata.updated(ConjureUpdatedChangeMetadata.of(
                    Bytes.from(PtBytes.toBytes("old")), Bytes.from(PtBytes.toBytes("new")))),
            2,
            ConjureChangeMetadata.deleted(ConjureDeletedChangeMetadata.of(Bytes.from(PtBytes.toBytes("deleted")))),
            3,
            ConjureChangeMetadata.created(ConjureCreatedChangeMetadata.of(Bytes.from(PtBytes.toBytes("created")))));
    private static ConjureLockRequestMetadata conjureLockRequestMetadata;

    @BeforeClass
    public static void setup() {
        KeyListChecksum checksum =
                IndexEncodingUtils.computeChecksum(ConjureLockRequestMetadataUtils.DEFAULT_CHECKSUM_TYPE, LOCK_LIST);
        conjureLockRequestMetadata = ConjureLockRequestMetadata.builder()
                .indexToChangeMetadata(CONJURE_LOCKS_WITH_METADATA)
                .checksumTypeId(checksum.type().getId())
                .checksumValue(Bytes.from(checksum.value()))
                .build();
    }

    @Test
    public void convertsToConjureCorrectly() {
        assertThat(ConjureLockRequestMetadataUtils.toConjureIndexEncoded(LOCK_SET, LOCK_REQUEST_METADATA).rhSide)
                .isEqualTo(conjureLockRequestMetadata);
    }

    @Test
    public void convertsFromConjureCorrectly() {
        assertThat(ConjureLockRequestMetadataUtils.fromConjureIndexEncoded(LOCK_LIST, conjureLockRequestMetadata))
                .isEqualTo(LOCK_REQUEST_METADATA);
    }
}
