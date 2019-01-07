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

package com.palantir.atlasdb.transaction.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

import com.palantir.atlasdb.coordination.CoordinationService;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadata;
import com.palantir.atlasdb.internalschema.persistence.CoordinationServices;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.timestamp.InMemoryTimestampService;

public class TransactionServicesTest {
    public static final int COMMIT_TS = 555;
    private final KeyValueService keyValueService = new InMemoryKeyValueService(true);
    private final CoordinationService<InternalSchemaMetadata> coordinationService
            = CoordinationServices.createDefault(keyValueService, new InMemoryTimestampService(), false);
    private final TransactionService transactionService = TransactionServices.createTransactionService(
            keyValueService, coordinationService);

    @Test
    public void valuesPutMayBeSubsequentlyRetrieved() {
        transactionService.putUnlessExists(1, COMMIT_TS);
        assertThat(transactionService.get(1)).isEqualTo(COMMIT_TS);
    }

    @Test
    public void canPutAndGetAtNegativeTimestamps() {
        transactionService.putUnlessExists(-1, COMMIT_TS);
        assertThat(transactionService.get(-1)).isEqualTo(COMMIT_TS);
    }

    @Test
    public void cannotPutNegativeValuesTwice() {
        transactionService.putUnlessExists(-1, COMMIT_TS);
        assertThatThrownBy(() -> transactionService.putUnlessExists(-1, COMMIT_TS + 1))
                .isInstanceOf(KeyAlreadyExistsException.class)
                .hasMessageContaining("already have a value for this timestamp");
        assertThat(transactionService.get(-1)).isEqualTo(COMMIT_TS);
    }
}
