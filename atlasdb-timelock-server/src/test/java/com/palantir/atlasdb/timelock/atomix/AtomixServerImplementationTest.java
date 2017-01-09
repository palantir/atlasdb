/**
 * Copyright 2017 Palantir Technologies
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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;

import org.junit.Ignore;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.timelock.ServerImplementationTest;
import com.palantir.atlasdb.timelock.config.ImmutableAtomixConfiguration;
import com.palantir.atlasdb.timelock.config.ImmutableClusterConfiguration;
import com.palantir.atlasdb.timelock.config.TimeLockServerConfiguration;

import io.atomix.catalyst.transport.Address;
import io.atomix.copycat.server.storage.StorageLevel;

@Ignore("Observed ConcurrentModificationException-related flakes (e.g. build #5407 on CircleCI)."
        + "Fixed in atomix/copycat#231, but not part of Copycat 1.1.4 which we use.")
public class AtomixServerImplementationTest extends ServerImplementationTest {
    private static final String LOCAL_ADDRESS_STRING = "localhost:12345";
    private static final Address LOCAL_ADDRESS = new Address(LOCAL_ADDRESS_STRING);
    private static final TimeLockServerConfiguration TIMELOCK_CONFIG = new TimeLockServerConfiguration(
            ImmutableAtomixConfiguration.builder()
                    .storageLevel(StorageLevel.MEMORY)
                    .build(),
            ImmutableClusterConfiguration.builder()
                    .localServer(LOCAL_ADDRESS_STRING)
                    .addServers(LOCAL_ADDRESS_STRING)
                    .build(),
            ImmutableSet.of("test"));

    @Override
    protected TimeLockServerConfiguration getConfiguration() {
        return TIMELOCK_CONFIG;
    }

    @Override
    protected void verifyPostStartupSuccess() {
        try {
            tryToConnectToAtomixPort();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    protected void verifyPostStartupFailure() {
        assertThatThrownBy(AtomixServerImplementationTest::tryToConnectToAtomixPort)
                .isInstanceOf(ConnectException.class);
    }

    @Override
    protected void verifyPostStop() {
        assertThatThrownBy(AtomixServerImplementationTest::tryToConnectToAtomixPort)
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("Connection refused");
    }

    private static void tryToConnectToAtomixPort() throws IOException {
        new Socket(LOCAL_ADDRESS.host(), LOCAL_ADDRESS.port());
    }
}
