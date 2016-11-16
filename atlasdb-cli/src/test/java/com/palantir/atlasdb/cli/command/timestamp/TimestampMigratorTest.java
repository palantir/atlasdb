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
package com.palantir.atlasdb.cli.command.timestamp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Before;
import org.junit.Test;

import com.palantir.timestamp.InMemoryTimestampService;

public class TimestampMigratorTest {
    private InMemoryTimestampService service1;
    private InMemoryTimestampService service2;

    private TimestampServicesProvider provider1;
    private TimestampServicesProvider provider2;

    private TimestampMigrator migrator;

    private static final long DELTA = 1_000_000;

    @Before
    public void setUp() {
        service1 = new InMemoryTimestampService();
        service2 = new InMemoryTimestampService();
        provider1 = TimestampServicesProviders.createFromSingleService(service1);
        provider2 = TimestampServicesProviders.createFromSingleService(service2);
        migrator = new TimestampMigrator(provider1, provider2);
    }

    @Test
    public void canMigrateTimestamps() {
        long ts1 = service1.getFreshTimestamp();
        service1.fastForwardTimestamp(ts1 + DELTA);
        migrator.migrateTimestamps();
        long ts2 = service2.getFreshTimestamp();
        assertThat(ts2 - ts1).isGreaterThan(DELTA);
    }

    @Test
    public void shouldPreventFurtherTimestampsBeingIssuedFromOldService() {
        service1.getFreshTimestamp();
        migrator.migrateTimestamps();
        assertThatThrownBy(service1::getFreshTimestamp).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void canMigrateOntoInvalidatedService() {
        long ts1 = service1.getFreshTimestamp();
        migrator.migrateTimestamps();
        long ts2 = service2.getFreshTimestamp();
        TimestampMigrator reverseMigrator = new TimestampMigrator(provider2, provider1);
        reverseMigrator.migrateTimestamps();
        long ts3 = service1.getFreshTimestamp();
        assertThat(ts1).isLessThan(ts2);
        assertThat(ts2).isLessThan(ts3);
    }
}
