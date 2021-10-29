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

package com.palantir.atlasdb.transaction.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import java.util.concurrent.ExecutionException;
import org.junit.Test;

public class PueTablingTransactionServiceTest {
    private final KeyValueService kvs = new InMemoryKeyValueService(true);
    private final TransactionService transactionService = PueTablingTransactionService.createV3(kvs);

    @Test
    public void smokeTest() throws ExecutionException, InterruptedException {
        assertThat(transactionService.get(8)).isNull();

        transactionService.putUnlessExists(8, 88);
        assertThat(transactionService.get(8)).isEqualTo(88);
        assertThat(transactionService.get(4)).isNull();
        assertThat(transactionService.get(ImmutableList.of(4L, 8L, 48L))).contains(Maps.immutableEntry(8L, 88L));

        transactionService.putUnlessExistsMultiple(ImmutableMap.of(6L, 66L, 9L, 9999L));
        assertThat(transactionService.get(6)).isEqualTo(66);
        assertThat(transactionService.get(ImmutableList.of(6L, 9L, 69L)))
                .contains(Maps.immutableEntry(6L, 66L), Maps.immutableEntry(9L, 9999L));
        assertThat(transactionService.getAsync(ImmutableList.of(6L, 9L, 69L)).get())
                .contains(Maps.immutableEntry(6L, 66L), Maps.immutableEntry(9L, 9999L));
    }
}
