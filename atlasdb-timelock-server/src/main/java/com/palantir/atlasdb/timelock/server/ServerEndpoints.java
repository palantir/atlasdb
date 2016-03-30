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

package com.palantir.atlasdb.timelock.server;

import java.util.function.Consumer;
import java.util.stream.Stream;

import javax.inject.Singleton;

import com.palantir.atlasdb.timelock.server.modules.ConfigModule;
import com.palantir.atlasdb.timelock.server.modules.EnvironmentModule;
import com.palantir.atlasdb.timelock.server.modules.TimeAndLockModule;
import com.palantir.atlasdb.timelock.server.modules.LeaderElectionModule;
import com.palantir.atlasdb.timelock.server.modules.qualifiers.Local;
import com.palantir.leader.LeaderElectionService;
import com.palantir.lock.RemoteLockService;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosLearner;
import com.palantir.timestamp.TimestampService;

import dagger.Component;

@Component(modules = {
        TimeAndLockModule.class,
        LeaderElectionModule.class,
        EnvironmentModule.class,
        ConfigModule.class,
})
@Singleton
public interface ServerEndpoints {
    TimestampService timestamp();
    RemoteLockService lock();
    LeaderElectionService leaderElection();
    @Local PaxosLearner paxosLearner();
    @Local PaxosAcceptor paxosAcceptor();

    default void forEach(Consumer<Object> consumer) {
        Stream.of(
                paxosLearner(),
                paxosAcceptor(),
                leaderElection(),
                timestamp(),
                lock()
        ).forEachOrdered(consumer);
    }
}
