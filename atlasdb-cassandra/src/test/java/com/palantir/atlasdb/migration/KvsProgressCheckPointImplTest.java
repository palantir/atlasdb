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

package com.palantir.atlasdb.migration;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.impl.TestResourceManager;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.impl.TransactionTestSetup;

public class KvsProgressCheckPointImplTest extends TransactionTestSetup {
    @ClassRule
    public static final TestResourceManager TRM = TestResourceManager.inMemory();

    private KvsProgressCheckPointImpl kvsProgressCheckPoint;
    private TransactionManager transactionManager;

    public KvsProgressCheckPointImplTest() {
        super(TRM, TRM);
    }

    @Before
    public void before() {
        transactionManager = getManager();
        kvsProgressCheckPoint = new KvsProgressCheckPointImpl(TRM.getDefaultKvs());
    }

    @After
    public void after() {
        TRM.getDefaultKvs().truncateTable(AtlasDbConstants.LIVE_MIGRATON_PROGRESS_TABLE);
    }

    @Test
    public void testFirstReadReturnsEmpty() {
        assertThat(getNextRow()).isEqualTo(PtBytes.EMPTY_BYTE_ARRAY);
    }

    @Test
    public void testWriteThenRead() {
        byte[] checkpointValue = PtBytes.toBytes("something");

        setNextRow(checkpointValue);

        assertThat(getNextRow()).contains(checkpointValue);
    }

    @Test
    public void testMultipleWritesThenRead() {
        byte[] expectedCheckpointValue = PtBytes.toBytes("something");

        setNextRow(PtBytes.toBytes("something-else"));
        setNextRow(PtBytes.toBytes("something-else-else"));
        setNextRow(expectedCheckpointValue);

        assertThat(getNextRow()).contains(expectedCheckpointValue);
    }

    @Test
    public void testNoCheckPointIsDifferentThanEndCheckpoint() {
        assertThat(getNextRow()).isEqualTo(PtBytes.EMPTY_BYTE_ARRAY);

        setNextRow(Optional.empty());
        assertThat(getNextRow()).isEmpty();
    }

    private void setNextRow(byte[] checkpointValue) {
        transactionManager.runTaskWithRetry(transaction -> {
            kvsProgressCheckPoint.setNextStartRow(transaction, Optional.of(checkpointValue));
            return null;
        });
    }

    private void setNextRow(Optional<byte[]> checkpointValue) {
        transactionManager.runTaskWithRetry(transaction -> {
            kvsProgressCheckPoint.setNextStartRow(transaction, checkpointValue);
            return null;
        });
    }

    private Optional<byte[]> getNextRow() {
        return transactionManager.runTaskWithRetry(
                transaction -> kvsProgressCheckPoint.getNextStartRow(transaction));
    }

}