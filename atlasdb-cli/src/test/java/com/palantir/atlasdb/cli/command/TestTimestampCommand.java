/**
 * Copyright 2015 Palantir Technologies
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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Scanner;

import org.joda.time.format.ISODateTimeFormat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.cli.command.timestamp.FetchTimestamp;
import com.palantir.atlasdb.cli.runner.InMemoryTestRunner;
import com.palantir.atlasdb.cli.runner.SingleBackendCliTestRunner;
import com.palantir.atlasdb.cli.services.AtlasDbServicesFactory;
import com.palantir.atlasdb.cli.services.DaggerTestAtlasDbServices;
import com.palantir.atlasdb.cli.services.ServicesConfigModule;
import com.palantir.atlasdb.cli.services.TestAtlasDbServices;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.timestamp.TimestampService;

public class TestTimestampCommand {

    private static LockDescriptor lock;
    private static AtlasDbServicesFactory moduleFactory;

    private static final String TIMESTAMP_FILE_PATH = "test.timestamp";
    private static final File TIMESTAMP_FILE = new File(TIMESTAMP_FILE_PATH);

    @BeforeClass
    public static void setup() throws Exception {
        lock = StringLockDescriptor.of("lock");
        moduleFactory = new AtlasDbServicesFactory() {
            @Override
            public TestAtlasDbServices connect(ServicesConfigModule servicesConfigModule) {
                return DaggerTestAtlasDbServices.builder()
                        .servicesConfigModule(servicesConfigModule)
                        .build();
            }
        };
    }

    @AfterClass
    public static void cleanUp() {
        if (TIMESTAMP_FILE.exists()){
            TIMESTAMP_FILE.delete();
        }
    }

    @Test
    public void testFreshToStdOut() throws Exception {
        genericTest(false, false);
    }

    @Test
    public void testImmutableToStdOut() throws Exception {
        genericTest(true, false);
    }

    @Test
    public void testFreshToFile() throws Exception {
        genericTest(false, true);
    }

    @Test
    public void testImmutableToFile() throws Exception {
        genericTest(true, true);
    }

    private SingleBackendCliTestRunner makeRunner(String... args) {
        return new InMemoryTestRunner(FetchTimestamp.class, args);
    }

    private void genericTest(boolean isImmutable, boolean isToFile) throws Exception {
        List<String> cliArgs = Lists.newArrayList("timestamp"); //group command
        if (isToFile) {
            cliArgs.add("-f");
            cliArgs.add(TIMESTAMP_FILE_PATH);
        }
        cliArgs.add("fetch");
        if (isImmutable) {
            cliArgs.add("-i");
        }
        cliArgs.add("-d"); //always test datetime

        try (SingleBackendCliTestRunner runner = makeRunner(cliArgs.toArray(new String[0]))) {
            TestAtlasDbServices services = runner.connect(moduleFactory);
            RemoteLockService rls = services.getLockService();
            TimestampService tss = services.getTimestampService();
            LockClient client = services.getTestLockClient();

            long immutableTs = tss.getFreshTimestamp();
            LockRequest request = LockRequest.builder(ImmutableSortedMap.of(
                    lock, LockMode.WRITE))
                    .withLockedInVersionId(immutableTs).doNotBlock().build();
            LockRefreshToken token = rls.lock(client.getClientId(), request);
            long lastFreshTs = tss.getFreshTimestamps(1000).getUpperBound();
            runAndVerify(runner, tss, isImmutable, isToFile, immutableTs, lastFreshTs);

            rls.unlock(token);
            lastFreshTs = tss.getFreshTimestamps(1000).getUpperBound();
            // there are no locks so we now expect immutable to just be a fresh 
            runAndVerify(runner, tss, false, isToFile, lastFreshTs, lastFreshTs);
        }
    }

    private void runAndVerify(SingleBackendCliTestRunner runner, TimestampService tss,
            boolean isImmutable, boolean isToFile, long immutableTs, long lastFreshTs) 
                    throws IOException {
        // prep
        if (isToFile && TIMESTAMP_FILE.exists()) {
            TIMESTAMP_FILE.delete();
        }

        // run the stuff
        long timestamp;
        String datetime;
        Scanner scanner = new Scanner(runner.run(true, false));
        if (!isToFile) {
            timestamp = Long.parseLong(scanner.findInLine("\\d+"));
            datetime = scanner.findInLine("\\d+.*").trim();
        } else {
            Preconditions.checkArgument(TIMESTAMP_FILE.exists(), "Timestamp file doesn't exist.");
            List<String> lines = Files.readAllLines(TIMESTAMP_FILE.toPath(), StandardCharsets.UTF_8);
            timestamp = Long.parseLong(lines.get(0));
            datetime = lines.get(1).trim();
        }
        scanner.close();

        // verify correctness
        ISODateTimeFormat.dateTimeNoMillis().parseDateTime(datetime);
        if (isImmutable) {
            Preconditions.checkArgument(timestamp == immutableTs);
            Preconditions.checkArgument(timestamp < lastFreshTs);
            Preconditions.checkArgument(timestamp < tss.getFreshTimestamp());
        } else {
            Preconditions.checkArgument(timestamp > immutableTs);
            Preconditions.checkArgument(timestamp > lastFreshTs);
            Preconditions.checkArgument(timestamp < tss.getFreshTimestamp());
        }
    }

}
