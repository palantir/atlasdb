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

package com.palantir.atlasdb.ete.abstracttests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.palantir.atlasdb.coordination.CoordinationResource;
import com.palantir.atlasdb.ete.utilities.EteExtension;
import org.junit.jupiter.api.Test;

public abstract class AbstractMultipleSchemaVersionsCoordinationTest {
    private static final int VERSION_ONE = 1;
    private static final int NEW_VERSION = 5888888;

    private final CoordinationResource coordinationResource =
            EteExtension.createClientToSingleNode(CoordinationResource.class);

    @Test
    public void transactionFailsUnderUnknownSchemaVersion() {
        coordinationResource.forceInstallNewTransactionsSchemaVersion(NEW_VERSION);
        AbstractCoordinationTest.assertTransactionsSchemaVersionIsNow(NEW_VERSION, coordinationResource);
        assertThat(coordinationResource.doTransactionAndReportOutcome()).isFalse();
    }

    @Test
    public void transactionOnKnownVersionFailsOnValueWithUnknownVersion() {
        coordinationResource.forceInstallNewTransactionsSchemaVersion(NEW_VERSION);
        AbstractCoordinationTest.assertTransactionsSchemaVersionIsNow(NEW_VERSION, coordinationResource);

        // writes to kvs via txn are blocked as transaction on NEW_VERSION will not be able to mark itself in
        // progress.
        assertThatCode(() -> coordinationResource.writeToKvsUnsafe(coordinationResource.getFreshTimestamp()))
                .doesNotThrowAnyException();

        coordinationResource.forceInstallNewTransactionsSchemaVersion(VERSION_ONE);
        AbstractCoordinationTest.assertTransactionsSchemaVersionIsNow(VERSION_ONE, coordinationResource);

        // This must still determine whether the transaction that started under a NEW_VERSION regime committed
        // or not, when performing conflict checking. We can't tell, hence we must fail.
        assertThat(coordinationResource.doTransactionAndReportOutcome()).isFalse();
    }
}
