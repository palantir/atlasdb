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

import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.timelock.TimeLockServerTest;
import com.palantir.timestamp.TimestampService;

import io.atomix.AtomixReplica;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.transport.local.LocalServerRegistry;
import io.atomix.catalyst.transport.local.LocalTransport;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import io.atomix.variables.DistributedLong;

public class AtomixTimestampServiceTest {

    private static final Address LOCAL_ADDRESS = new Address("localhost", 8700);
    private static final String TEST_KEY = TimeLockServerTest.class.getName();

    private static AtomixReplica replica;
    private static Transport transport;

    private TimestampService timestampService;

    @BeforeClass
    public static void setUpClass() {
        transport = new LocalTransport(new LocalServerRegistry());
        replica = AtomixReplica.builder(LOCAL_ADDRESS)
                .withStorage(Storage.builder()
                        .withDirectory("var/data/atomix")
                        .withStorageLevel(StorageLevel.DISK)
                        .build())
                .withTransport(transport)
                .build();
        replica.bootstrap().join();
    }

    @Before
    public void setUp() {
        DistributedLong distributedLong = Futures.getUnchecked(replica.getLong(TEST_KEY));
        distributedLong.set(0L);
        timestampService = new AtomixTimestampService(distributedLong);
    }

    @Test
    public void timestampsShouldBeIncreasing() {
        long ts1 = timestampService.getFreshTimestamp();
        long ts2 = timestampService.getFreshTimestamp();

        assertThat(ts1).isLessThan(ts2);
    }

    @Test
    public void shouldThrowIfRequestingNegativeNumbersOfTimestamps() {
        assertThatThrownBy(() -> timestampService.getFreshTimestamps(-1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void shouldThrowIfRequestingZeroTimestamps() {
        assertThatThrownBy(() -> timestampService.getFreshTimestamps(0)).isInstanceOf(IllegalArgumentException.class);
    }

    @AfterClass
    public static void tearDownClass() {
        transport.close();
    }

}
