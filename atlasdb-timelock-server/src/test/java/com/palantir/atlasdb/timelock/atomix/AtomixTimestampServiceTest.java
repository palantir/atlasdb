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

import com.palantir.timestamp.TimestampRange;

import io.atomix.AtomixReplica;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.local.LocalServerRegistry;
import io.atomix.catalyst.transport.local.LocalTransport;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import io.atomix.variables.DistributedLong;

public class AtomixTimestampServiceTest {
    private static final Address LOCAL_ADDRESS = new Address("localhost", 8700);
    private static final String CLIENT_KEY = "client";

    private static final AtomixReplica ATOMIX_REPLICA = AtomixReplica.builder(LOCAL_ADDRESS)
            .withStorage(Storage.builder()
                    .withStorageLevel(StorageLevel.MEMORY)
                    .build())
            .withTransport(new LocalTransport(new LocalServerRegistry()))
            .build();

    private AtomixTimestampService timestampService;

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
    }

    @Test
    public void timestampsShouldBeIncreasing() {
        long ts1 = timestampService.getFreshTimestamp();
        long ts2 = timestampService.getFreshTimestamp();

        assertThat(ts1).isLessThan(ts2);
    }

    @Test
    public void canRequestTimestampRange() {
        int expectedNumTimestamps = 5;
        TimestampRange range = timestampService.getFreshTimestamps(expectedNumTimestamps);

        long actualNumTimestamps = range.getUpperBound() - range.getLowerBound() + 1;
        assertThat(actualNumTimestamps)
                .withFailMessage("Expected %d timestamps, got %d timestamps. (The returned range was: %d-%d)",
                        expectedNumTimestamps, actualNumTimestamps, range.getLowerBound(), range.getUpperBound())
                .isEqualTo(expectedNumTimestamps);
    }

    @Test
    public void shouldThrowIfRequestingNegativeNumbersOfTimestamps() {
        assertThatThrownBy(() -> timestampService.getFreshTimestamps(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void shouldThrowIfRequestingZeroTimestamps() {
        assertThatThrownBy(() -> timestampService.getFreshTimestamps(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void shouldThrowIfRequestingTooManyTimestamps() {
        assertThatThrownBy(() -> timestampService.getFreshTimestamps(AtomixTimestampService.MAX_GRANT_SIZE + 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void shouldDoNothingIfFastForwardingToThePast() {
        long currentTimestamp = timestampService.getFreshTimestamp();
        timestampService.fastForwardTimestamp(Integer.MIN_VALUE);
        long nextTimestamp = timestampService.getFreshTimestamp();
        assertThat(currentTimestamp).isLessThan(nextTimestamp);
    }

    @Test
    public void shouldOnlyReturnTimestampsAfterNewMinimumAfterFastForward() {
        long delta = 10000L;

        long currentTimestamp = timestampService.getFreshTimestamp();
        timestampService.fastForwardTimestamp(currentTimestamp + delta);
        long nextTimestamp = timestampService.getFreshTimestamp();
        assertThat(nextTimestamp - currentTimestamp).isGreaterThan(delta);
    }

    @Test
    public void shouldThrowIfTryingToUpdateTimestampToThePast() {
        long currentTimestamp = timestampService.getFreshTimestamp();
        assertThatThrownBy(() -> timestampService.attemptTimestampUpdate(Integer.MIN_VALUE, currentTimestamp))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void shouldNotTryToUpdateTimestampWithIncorrectCurrentValue() {
        long currentTimestamp = timestampService.getFreshTimestamp();
        assertThat(timestampService.attemptTimestampUpdate(currentTimestamp, currentTimestamp - 1)).isEqualTo(false);
    }

    @Test
    public void shouldUpdateTimestampWithCorrectCurrentValue() {
        long currentTimestamp = timestampService.getFreshTimestamp();
        assertThat(timestampService.attemptTimestampUpdate(currentTimestamp + 1, currentTimestamp)).isEqualTo(true);
    }
}
