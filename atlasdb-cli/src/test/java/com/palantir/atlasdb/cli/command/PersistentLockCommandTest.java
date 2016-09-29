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
package com.palantir.atlasdb.cli.command;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.BeforeClass;
import org.junit.Test;

import com.palantir.atlasdb.cli.runner.InMemoryTestRunner;
import com.palantir.atlasdb.cli.runner.SingleBackendCliTestRunner;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.persistentlock.PersistentLock;
import com.palantir.atlasdb.persistentlock.PersistentLockIsTakenException;
import com.palantir.atlasdb.persistentlock.PersistentLockName;
import com.palantir.atlasdb.services.AtlasDbServicesFactory;
import com.palantir.atlasdb.services.ServicesConfigModule;
import com.palantir.atlasdb.services.test.DaggerTestAtlasDbServices;
import com.palantir.atlasdb.services.test.TestAtlasDbServices;

import io.airlift.airline.Command;

public class PersistentLockCommandTest {
    private static final String LOCK_COMMAND_NAME = PersistentLockCommand.class.getAnnotation(Command.class).name();
    private static final PersistentLockName LOCK_NAME = PersistentLockName.of("someLockName");

    private static AtlasDbServicesFactory moduleFactory;

    @BeforeClass
    public static void setup() throws Exception {
        moduleFactory = new AtlasDbServicesFactory() {
            @Override
            public TestAtlasDbServices connect(ServicesConfigModule servicesConfigModule) {
                return DaggerTestAtlasDbServices.builder()
                        .servicesConfigModule(servicesConfigModule)
                        .build();
            }
        };
    }

    private InMemoryTestRunner makeRunner(String... args) {
        return new InMemoryTestRunner(PersistentLockCommand.class, args);
    }

    @Test
    public void acquireLock() throws Exception {
        try (SingleBackendCliTestRunner runner = makeRunner(LOCK_COMMAND_NAME, "--acquire", LOCK_NAME.name())) {
            TestAtlasDbServices services = runner.connect(moduleFactory);

            runner.run();

            List<PersistentLockName> lockNames = getAllLockNames(services.getKeyValueService());
            assertThat(lockNames, contains(LOCK_NAME));
        }
    }

    @Test
    public void listLocks() throws Exception {
        try (SingleBackendCliTestRunner runner = makeRunner(LOCK_COMMAND_NAME, "--list")) {
            TestAtlasDbServices services = runner.connect(moduleFactory);
            writeLock(services.getKeyValueService());

            String stdout = runner.run();

            assertThat(stdout, containsString(LOCK_NAME.name()));
        }
    }

    private List<PersistentLockName> getAllLockNames(KeyValueService keyValueService) {
        PersistentLock persistentLock = new PersistentLock(keyValueService);

        return persistentLock.allLockEntries().stream()
                .map(lockEntry -> lockEntry.lockName())
                .collect(Collectors.toList());
    }

    private void writeLock(KeyValueService keyValueService) throws PersistentLockIsTakenException {
        PersistentLock persistentLock = new PersistentLock(keyValueService);
        persistentLock.acquireLock(LOCK_NAME, "Some reason");
    }
}
