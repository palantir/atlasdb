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

package com.palantir.atlasdb.internalschema;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.coordination.ValueAndBound;
import com.palantir.atlasdb.internalschema.persistence.CoordinationServices;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.timestamp.InMemoryTimestampService;

public class AggressiveConcurrentUpdateTest {
    private final ExecutorService service = Executors.newFixedThreadPool(8);

    @Test
    public void correctStateAfterAggressiveConcurrentUpdates() {
        TransactionSchemaManager manager = new TransactionSchemaManager(
                CoordinationServices.createDefault(
                        new InMemoryKeyValueService(true),
                        new InMemoryTimestampService(),
                        false));
        List<Future> futures = Lists.newArrayList();
        Set<Optional<ValueAndBound<InternalSchemaMetadata>>> snapshots = Sets.newHashSet();

        for (int i = 0; i < 1000; i++) {
            int version = i;
            futures.add(service.submit(() -> {
                manager.tryInstallNewTransactionsSchemaVersion(version);
                snapshots.add(manager.getCoordinationService().getLastKnownLocalValue());
            }));
        }
        futures.forEach(Futures::getUnchecked);
        // TODO (jkong): Validate snapshots are correct.
    }
}
