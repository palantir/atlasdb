/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.timelock.clock;

import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import javax.net.ssl.SSLSocketFactory;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.timelock.clock.ClockServiceImpl;
import com.palantir.atlasdb.timelock.clock.ClockSkewMonitor;
import com.palantir.remoting3.config.ssl.SslSocketFactories;
import com.palantir.timelock.config.TimeLockInstallConfiguration;
import com.palantir.timelock.paxos.PaxosRemotingUtils;

public class ClockSkewMonitorCreator {
    private final Set<String> remoteServers;
    private final Optional<SSLSocketFactory> optionalSecurity;
    private final Consumer<Object> registrar;

    @VisibleForTesting
    ClockSkewMonitorCreator(Set<String> remoteServers,
            Optional<SSLSocketFactory> optionalSecurity,
            Consumer<Object> registrar) {
        this.remoteServers = remoteServers;
        this.optionalSecurity = optionalSecurity;
        this.registrar = registrar;
    }

    public static ClockSkewMonitorCreator create(
            TimeLockInstallConfiguration install,
            Consumer<Object> registrar) {
        Set<String> remoteServers = PaxosRemotingUtils.getRemoteServerPaths(install);
        Optional<SSLSocketFactory> optionalSecurity =
                PaxosRemotingUtils.getSslConfigurationOptional(install).map(SslSocketFactories::createSslSocketFactory);

        return new ClockSkewMonitorCreator(remoteServers, optionalSecurity, registrar);
    }

    public void registerClockServices() {
        runClockSkewMonitorInBackground();
        registrar.accept(new ClockServiceImpl());
    }

    private void runClockSkewMonitorInBackground() {
        ClockSkewMonitor.create(remoteServers, optionalSecurity).runInBackground();
    }
}
