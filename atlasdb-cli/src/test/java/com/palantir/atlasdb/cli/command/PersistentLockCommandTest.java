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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

import static com.palantir.atlasdb.persistentlock.PersistentLock.LOCKS_TIMESTAMP;

import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cli.runner.InMemoryTestRunner;
import com.palantir.atlasdb.cli.runner.SingleBackendCliTestRunner;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.persistentlock.LockEntry;
import com.palantir.atlasdb.persistentlock.PersistentLock;
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
    public static final long CUSTOM_LOCK_ID = 1234;

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

            assertThat(getAllLocks(services.getKeyValueService()).size(), equalTo(1));
        }
    }

    @Test
    public void releaseLock() throws Exception {
        try (SingleBackendCliTestRunner runner = makeRunner(
                LOCK_COMMAND_NAME, "--release", LOCK_NAME.name(), "--lockId", String.valueOf(CUSTOM_LOCK_ID))) {
            TestAtlasDbServices services = runner.connect(moduleFactory);
            KeyValueService keyValueService = services.getKeyValueService();
            insertLockWithId(keyValueService, CUSTOM_LOCK_ID);

            runner.run();

            assertThat(getAllLocks(keyValueService).size(), equalTo(0));
        }
    }

    @Test
    public void listLocks() throws Exception {
        try (SingleBackendCliTestRunner runner = makeRunner(LOCK_COMMAND_NAME, "--list")) {
            TestAtlasDbServices services = runner.connect(moduleFactory);
            insertLockWithId(services.getKeyValueService(), CUSTOM_LOCK_ID);

            String stdout = runner.run();

            assertThat(stdout, containsString(LOCK_NAME.name()));
        }
    }


    private Set<LockEntry> getAllLocks(KeyValueService keyValueService) {
        PersistentLock persistentLock = PersistentLock.create(keyValueService);

        return persistentLock.allLockEntries();
    }

    private void insertLockWithId(KeyValueService keyValueService, long lockId) {
        LockEntry customEntry = LockEntry.of(LOCK_NAME, lockId, "some reason");
        keyValueService.createTable(AtlasDbConstants.PERSISTED_LOCKS_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
        keyValueService.put(AtlasDbConstants.PERSISTED_LOCKS_TABLE, customEntry.insertionMap(), LOCKS_TIMESTAMP);
    }
}
