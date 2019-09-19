/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.cassandra;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.containers.CassandraResource;
import com.palantir.flake.ShouldRetry;
import com.palantir.timestamp.MultipleRunningTimestampServiceError;
import com.palantir.timestamp.TimestampBoundStore;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

@ShouldRetry
public class CassandraTimestampStoreInvalidatorIntegrationTest {
    @ClassRule
    public static final CassandraResource CASSANDRA = new CassandraResource();

    private static final long ONE_MILLION = 1_000_000;

    private final CassandraKeyValueService kv = CASSANDRA.getDefaultKvs();

    private final CassandraTimestampStoreInvalidator invalidator = CassandraTimestampStoreInvalidator.create(kv);

    @Before
    public void setUp() {
        kv.createTable(AtlasDbConstants.TIMESTAMP_TABLE,
                CassandraTimestampBoundStore.TIMESTAMP_TABLE_METADATA.persistToBytes());
        kv.truncateTable(AtlasDbConstants.TIMESTAMP_TABLE);
    }

    @Test
    public void canBackupTimestampTableIfItDoesNotExist() {
        kv.dropTable(AtlasDbConstants.TIMESTAMP_TABLE);
        assertThat(invalidator.backupAndInvalidate()).isEqualTo(CassandraTimestampUtils.INITIAL_VALUE);
    }

    @Test
    public void canBackupTimestampTableIfItExistsWithNoData() {
        assertThat(invalidator.backupAndInvalidate()).isEqualTo(CassandraTimestampUtils.INITIAL_VALUE);
    }

    @Test
    public void canBackupTwice() {
        assertThat(invalidator.backupAndInvalidate()).isEqualTo(CassandraTimestampUtils.INITIAL_VALUE);
        assertThat(invalidator.backupAndInvalidate()).isEqualTo(CassandraTimestampUtils.INITIAL_VALUE);
    }

    @Test
    public void canBackupTimestampTableIfItExistsWithData() {
        long limit = getBoundAfterTakingOutOneMillionTimestamps();
        assertThat(invalidator.backupAndInvalidate()).isEqualTo(limit);
    }

    @Test
    public void canBackupAndRestoreTimestampTable() {
        TimestampBoundStore timestampBoundStore = CassandraTimestampBoundStore.create(kv);
        long limit = timestampBoundStore.getUpperLimit();
        timestampBoundStore.storeUpperLimit(limit + ONE_MILLION);
        invalidator.backupAndInvalidate();
        invalidator.revalidateFromBackup();
        assertThat(timestampBoundStore.getUpperLimit()).isEqualTo(limit + ONE_MILLION);
    }

    @Test
    public void restoringValidTimestampTableIsANoOp() {
        assertWeCanReadInitialValue();
        invalidator.revalidateFromBackup();
        assertWeCanReadInitialValue();
        invalidator.revalidateFromBackup();
        assertWeCanReadInitialValue();
    }

    @Test
    public void restoringTimestampTableIfItDoesNotExistIsANoOp() {
        kv.dropTable(AtlasDbConstants.TIMESTAMP_TABLE);
        invalidator.revalidateFromBackup();
        assertWeCanReadInitialValue();
    }

    @Test
    public void restoringTimestampTableWithNoDataIsANoOp() {
        invalidator.revalidateFromBackup();
        assertWeCanReadInitialValue();
    }

    @Test
    public void multipleInvalidatorsForSameKeyValueServiceAllReturnSameResults() {
        long limit = getBoundAfterTakingOutOneMillionTimestamps();
        CassandraTestTools.executeInParallelOnExecutorService(() ->
                assertThat(CassandraTimestampStoreInvalidator.create(kv).backupAndInvalidate()).isEqualTo(limit));
    }

    /**
     * Consistent results here mean that:
     *   (1) all calls to backupAndInvalidate() return the same value V, and
     *   (2) this value V is the maximum of all successfully stored timestamp bounds.
     */
    @Test
    public void invalidationDuringTimestampIssuanceYieldsConsistentResults() {
        Set<Long> backedUpValues = ConcurrentHashMap.newKeySet();
        AtomicLong maxSuccessfulBound = new AtomicLong(CassandraTimestampUtils.INITIAL_VALUE);

        CassandraTestTools.executeInParallelOnExecutorService(() -> {
            CassandraTimestampStoreInvalidator storeInvalidator = CassandraTimestampStoreInvalidator.create(kv);
            try {
                if (ThreadLocalRandom.current().nextBoolean()) {
                    backedUpValues.add(storeInvalidator.backupAndInvalidate());
                } else {
                    maxSuccessfulBound.accumulateAndGet(getBoundAfterTakingOutOneMillionTimestamps(), Math::max);
                }
            } catch (IllegalArgumentException | IllegalStateException | MultipleRunningTimestampServiceError error) {
                // Can arise if trying to manipulate the timestamp bound during/after an invalidation. This is fine.
            }
        });

        if (!backedUpValues.isEmpty()) {
            invalidator.revalidateFromBackup();
            assertThat(Iterables.getOnlyElement(backedUpValues)).isEqualTo(maxSuccessfulBound.get());
            assertWeCanReadTimestamp(maxSuccessfulBound.get());
        }
        // 2^-32 chance that nothing got backed up; accept in this case.
    }

    private void assertWeCanReadInitialValue() {
        assertWeCanReadTimestamp(CassandraTimestampUtils.INITIAL_VALUE);
    }

    private void assertWeCanReadTimestamp(long expectedTimestamp) {
        TimestampBoundStore timestampBoundStore = CassandraTimestampBoundStore.create(kv);
        assertThat(timestampBoundStore.getUpperLimit()).isEqualTo(expectedTimestamp);
    }

    private long getBoundAfterTakingOutOneMillionTimestamps() {
        TimestampBoundStore timestampBoundStore = CassandraTimestampBoundStore.create(kv);
        long newLimit = timestampBoundStore.getUpperLimit() + ONE_MILLION;
        timestampBoundStore.storeUpperLimit(newLimit);
        return newLimit;
    }
}
