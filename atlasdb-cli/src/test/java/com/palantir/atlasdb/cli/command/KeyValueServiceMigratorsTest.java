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
package com.palantir.atlasdb.cli.command;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.TimestampService;

public class KeyValueServiceMigratorsTest {
    private static final long FUTURE_TIMESTAMP = 3141592653589L;

    private final AtlasDbServices oldServices = createMockAtlasDbServices();
    private final AtlasDbServices newServices = createMockAtlasDbServices();

    private final ImmutableMigratorSpec.Builder migratorSpecBuilder = ImmutableMigratorSpec.builder()
            .fromServices(oldServices)
            .toServices(newServices);

    @Test
    public void setupMigratorFastForwardsTimestamp() throws Exception {
        KeyValueServiceMigrators.getTimestampManagementService(oldServices).fastForwardTimestamp(FUTURE_TIMESTAMP);
        assertThat(newServices.getTimestampService().getFreshTimestamp()).isLessThan(FUTURE_TIMESTAMP);

        KeyValueServiceMigrators.setupMigrator(migratorSpecBuilder.build());

        assertThat(newServices.getTimestampService().getFreshTimestamp()).isGreaterThanOrEqualTo(FUTURE_TIMESTAMP);
    }

    @Test
    public void setupMigratorCommitsOneTransaction() throws Exception {
        KeyValueServiceMigrators.setupMigrator(migratorSpecBuilder.build());

        ArgumentCaptor<Long> startTimestampCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Long> commitTimestampCaptor = ArgumentCaptor.forClass(Long.class);
        verify(newServices.getTransactionService()).putUnlessExists(
                startTimestampCaptor.capture(),
                commitTimestampCaptor.capture());
        assertThat(startTimestampCaptor.getValue()).isLessThan(commitTimestampCaptor.getValue());
        assertThat(commitTimestampCaptor.getValue()).isLessThan(newServices.getTimestampService().getFreshTimestamp());
    }

    @Test
    public void throwsIfSpecifyingNegativeThreads() throws Exception {
        assertThatThrownBy(() -> migratorSpecBuilder
                .threads(-2)
                .build()).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void throwsIfSpecifyingNegativeBatchSize() throws Exception {
        assertThatThrownBy(() -> migratorSpecBuilder
                .batchSize(-2)
                .build()).isInstanceOf(IllegalArgumentException.class);
    }

    private static AtlasDbServices createMockAtlasDbServices() {
        TimestampService timestampService = new InMemoryTimestampService();

        AtlasDbServices mockServices = mock(AtlasDbServices.class);
        when(mockServices.getTimestampService()).thenReturn(timestampService);
        when(mockServices.getTransactionService()).thenReturn(mock(TransactionService.class));
        return mockServices;
    }
}
