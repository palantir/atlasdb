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
package com.palantir.atlasdb.timelock.atomix;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.atomix.AtomixReplica;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.local.LocalServerRegistry;
import io.atomix.catalyst.transport.local.LocalTransport;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import io.atomix.variables.DistributedLong;

public class AtomixTimestampAdministrationServiceTest {
    private static final Address LOCAL_ADDRESS = new Address("localhost", 8700);
    private static final String CLIENT_KEY = "client";

    private static final AtomixReplica ATOMIX_REPLICA = AtomixReplica.builder(LOCAL_ADDRESS)
            .withStorage(Storage.builder()
                    .withStorageLevel(StorageLevel.MEMORY)
                    .build())
            .withTransport(new LocalTransport(new LocalServerRegistry()))
            .build();

    private AtomixTimestampService timestampService;
    private AtomixTimestampAdminService administrationService;

    @BeforeClass
    public static void startAtomix() {
        ATOMIX_REPLICA.bootstrap().join();
    }

    @AfterClass
    public static void stopAtomix() {
        ATOMIX_REPLICA.leave();
    }

    @Before
    public void setupTimestampService() {
        DistributedLong distributedLong = DistributedValues.getTimestampForClient(ATOMIX_REPLICA, CLIENT_KEY);
        timestampService = new AtomixTimestampService(distributedLong);
        administrationService = new AtomixTimestampAdminService(distributedLong);
    }

    @Test
    public void shouldReturnUpperBound() {
        long freshTimestamp = timestampService.getFreshTimestamp();
        assertThat(administrationService.getUpperBoundTimestamp()).isGreaterThanOrEqualTo(freshTimestamp);
    }

    @Test
    public void shouldNotIssueTimestampsFastForwardedPast() {
        long oldValue = timestampService.getFreshTimestamp();
        long delta = 10000L;
        administrationService.fastForwardTimestamp(oldValue + delta);
        long newValue = timestampService.getFreshTimestamp();
        assertThat(newValue - oldValue).isGreaterThan(delta);
    }

    @Test
    public void shouldDoNothingIfFastForwardingToThePast() {
        long oldValue = timestampService.getFreshTimestamp();
        long delta = 10000L;
        administrationService.fastForwardTimestamp(oldValue - delta);
        long newValue = timestampService.getFreshTimestamp();
        assertThat(newValue).isGreaterThan(oldValue);
    }

    @Test
    public void shouldThrowOnInvalidatedTimestampService() {
        long oldValue = timestampService.getFreshTimestamp();
        try {
            administrationService.invalidateTimestamps();
            assertThatThrownBy(timestampService::getFreshTimestamp).isInstanceOf(IllegalStateException.class);
        } finally {
            administrationService.fastForwardTimestamp(oldValue);
        }
    }

    @Test
    public void canRevalidateServiceViaFastForward() {
        long oldValue = timestampService.getFreshTimestamp();
        try {
            administrationService.invalidateTimestamps();
            assertThatThrownBy(timestampService::getFreshTimestamp).isInstanceOf(IllegalStateException.class);
            administrationService.fastForwardTimestamp(0L);
            long newTimestamp = timestampService.getFreshTimestamp();
            assertThat(newTimestamp).isGreaterThan(0L);
        } finally {
            administrationService.fastForwardTimestamp(oldValue);
        }
    }
}
