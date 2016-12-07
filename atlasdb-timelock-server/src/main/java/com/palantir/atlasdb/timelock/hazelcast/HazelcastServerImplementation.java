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
package com.palantir.atlasdb.timelock.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ReplicatedMap;
import com.palantir.atlasdb.timelock.ServerImplementation;
import com.palantir.atlasdb.timelock.TimeLockServices;
import com.palantir.atlasdb.timelock.config.TimeLockServerConfiguration;
import com.palantir.lock.impl.LockServiceImpl;

public class HazelcastServerImplementation implements ServerImplementation {
    private HazelcastInstance hazelcastInstance;

    @Override
    public void onStart(TimeLockServerConfiguration configuration) {
        Config config = new Config();
        config.addMapConfig(new MapConfig()
                .setName("timestamps")
                .setMapStoreConfig(new MapStoreConfig()
                        .setImplementation(new HazelcastTimestampMapStore())
                        .setWriteDelaySeconds(0)));
        hazelcastInstance = Hazelcast.newHazelcastInstance(config);
    }

    @Override
    public void onStop() {
        if (hazelcastInstance != null) {
            hazelcastInstance.shutdown();
            hazelcastInstance = null;
        }
    }

    @Override
    public void onFail() {
        onStop();
    }

    @Override
    public TimeLockServices createInvalidatingTimeLockServices(String client) {
        ReplicatedMap<String, Long> timestamps = hazelcastInstance.getReplicatedMap("timestamps");
        return TimeLockServices.create(
                new HazelcastTimestampService(delta -> {
                    while (true) {
                        Long oldValue = timestamps.get("timestamp/" + client);
                        Long newValue = oldValue + delta;
                        if (timestamps.replace("timestamp/" + client, oldValue, newValue)) {
                            return oldValue;
                        }
                    }
                }),
                LockServiceImpl.create());
    }
}
