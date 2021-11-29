/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl;

import com.palantir.atlasdb.internalschema.TransactionSchemaManager;
import com.palantir.atlasdb.internalschema.persistence.CoordinationServices;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.ManagedTimestampService;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TransactionSchemaVersionEnforcementTest {
    private final KeyValueService keyValueService = new InMemoryKeyValueService(true);
    private final ManagedTimestampService managedTimestampService = new InMemoryTimestampService();
    private final TransactionSchemaManager schemaManager = new TransactionSchemaManager(
            CoordinationServices.createDefault(keyValueService, managedTimestampService::getFreshTimestamp, false));

    @Test
    public void canEnforceSchemaVersions() {
        long oldTimestamp = managedTimestampService.getFreshTimestamp();
        TransactionSchemaVersionEnforcement.ensureTransactionsGoingForwardHaveSchemaVersion(
                schemaManager, managedTimestampService, managedTimestampService, 2);
        assertThat(schemaManager.getTransactionsSchemaVersion(oldTimestamp)).isEqualTo(1);
        assertThat(schemaManager.getTransactionsSchemaVersion(managedTimestampService.getFreshTimestamp()))
                .isEqualTo(2);
        TransactionSchemaVersionEnforcement.ensureTransactionsGoingForwardHaveSchemaVersion(
                schemaManager, managedTimestampService, managedTimestampService, 3);
        assertThat(schemaManager.getTransactionsSchemaVersion(managedTimestampService.getFreshTimestamp()))
                .isEqualTo(3);
        TransactionSchemaVersionEnforcement.ensureTransactionsGoingForwardHaveSchemaVersion(
                schemaManager, managedTimestampService, managedTimestampService, 2);
        assertThat(schemaManager.getTransactionsSchemaVersion(managedTimestampService.getFreshTimestamp()))
                .isEqualTo(2);
    }
}
