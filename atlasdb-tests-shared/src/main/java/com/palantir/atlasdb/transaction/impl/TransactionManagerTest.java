/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.transaction.impl;


import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.common.concurrent.PTExecutors;

public class TransactionManagerTest extends TransactionTestSetup {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldNotCloseTransactionManagerMultipleTimes() throws Exception {
        txMgr.close();
        exception.expect(IllegalStateException.class);
        exception.expectMessage("Operations cannot be performed on closed TransactionManager");

        txMgr.close();
    }

    @Test
    public void shouldNotRunTaskWithRetryWithClosedTransactionManager() throws Exception {
        txMgr.close();
        exception.expect(IllegalStateException.class);
        exception.expectMessage("Operations cannot be performed on closed TransactionManager");

        txMgr.runTaskWithRetry(new TransactionTask<Void, RuntimeException>() {
            @Override
            public Void execute(Transaction t) throws RuntimeException {
                put(t, "row1", "col1", "v1");
                return null;
            }
        });
    }

    @Test
    public void shouldNotRunTaskThrowOnConflictWithClosedTransactionManager() throws Exception {
        txMgr.close();
        exception.expect(IllegalStateException.class);
        exception.expectMessage("Operations cannot be performed on closed TransactionManager");

        txMgr.runTaskThrowOnConflict(new TransactionTask<Void, RuntimeException>() {
            @Override
            public Void execute(Transaction t) throws RuntimeException {
                put(t, "row1", "col1", "v1");
                return null;
            }
        });
    }

    @Test
    public void shouldNotRunTaskReadOnlyWithClosedTransactionManager() throws Exception {
        txMgr.close();
        exception.expect(IllegalStateException.class);
        exception.expectMessage("Operations cannot be performed on closed TransactionManager");

        txMgr.runTaskReadOnly(new TransactionTask<Void, RuntimeException>() {
            @Override
            public Void execute(Transaction t) throws RuntimeException {
                put(t, "row1", "col1", "v1");
                return null;
            }
        });
    }

    @Override
    protected KeyValueService getKeyValueService() {
        return new InMemoryKeyValueService(false, PTExecutors.newSingleThreadExecutor(PTExecutors.newNamedThreadFactory(true)));
    }
}
