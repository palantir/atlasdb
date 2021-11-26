/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.cli.command;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.base.Splitter;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cleaner.GlobalClock;
import com.palantir.atlasdb.cleaner.KeyValueServicePuncherStore;
import com.palantir.atlasdb.cleaner.Puncher;
import com.palantir.atlasdb.cleaner.PuncherStore;
import com.palantir.atlasdb.cleaner.SimplePuncher;
import com.palantir.atlasdb.cli.command.timestamp.FetchTimestamp;
import com.palantir.atlasdb.cli.runner.InMemoryTestRunner;
import com.palantir.atlasdb.cli.runner.SingleBackendCliTestRunner;
import com.palantir.atlasdb.services.AtlasDbServicesFactory;
import com.palantir.atlasdb.services.ServicesConfigModule;
import com.palantir.atlasdb.services.test.DaggerTestAtlasDbServices;
import com.palantir.atlasdb.services.test.TestAtlasDbServices;
import com.palantir.common.time.Clock;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockService;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.timestamp.TimestampService;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Scanner;
import org.apache.commons.io.FileUtils;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestTimestampCommand {

    private static LockDescriptor lock;
    private static AtlasDbServicesFactory moduleFactory;
    private static List<String> cliArgs;

    @BeforeClass
    public static void oneTimeSetup() throws Exception {
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

    @Before
    public void setup() throws IOException {
        cliArgs = Lists.newArrayList("timestamp");
        cleanup();
    }

    @After
    public void cleanup() throws IOException {
        Files.deleteIfExists(new File("test.timestamp").toPath());
        deleteDirectoryIfExists("existing-dir");
        deleteDirectoryIfExists("missing-dir");
        deleteDirectoryIfExists("readonly-dir");
    }

    private void deleteDirectoryIfExists(String directory) throws IOException {
        File dir = new File(directory);
        if (!dir.exists()) {
            return;
        }
        if (!dir.canWrite()) {
            assertThat(dir.setWritable(true)).isTrue();
        }
        FileUtils.deleteDirectory(dir);
    }

    @Parameterized.Parameter(value = 0)
    public boolean isImmutable;

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {{false}, {true}});
    }

    private SingleBackendCliTestRunner makeRunner(String... args) {
        return new InMemoryTestRunner(FetchTimestamp.class, args);
    }

    private interface Verifier {
        void verify(
                SingleBackendCliTestRunner runner,
                TimestampService tss,
                long immutableTs,
                long prePunch,
                long lastFreshTs,
                boolean shouldTestImmutable)
                throws Exception;
    }

    /*
     * Test that the various values produced by fetching either a fresh timestamp or
     * an immutable timestamp are correct relative to one another as well as the current
     * state of locks held by the lock service. isImmutable designates whether or not we
     * provided the argument to fetch the immutable timestamp rather than a fresh one.
     *
     * Also make sure that the wall clock times associated with those timestamps are correct
     * according to the current state of the _punch table.  We always provide the argument
     * to output this and thus always check it.
     *
     * Additionally, check that the formatting of commands that are writing the timestamps
     * to a file for the purposes of scripting these commands is also correct.
     */
    @Test
    public void testWithoutFile() throws Exception {
        cliArgs.add("fetch");
        if (isImmutable) {
            cliArgs.add("-i");
        }
        cliArgs.add("-d");

        runAndVerifyCli((runner, tss, immutableTs, prePunch, lastFreshTs, shouldTestImmutable) -> {
            String output = runner.run(true, false);
            try {
                Scanner scanner = new Scanner(output);
                String shouldBeDeprecationLine = scanner.nextLine();
                verifyDeprecation(shouldBeDeprecationLine);
                long timestamp = getTimestampFromStdout(scanner);
                scanner.nextLine();
                long wallClockTimestamp = getWallClockTimestamp(scanner);
                scanner.close();
                if (shouldTestImmutable && isImmutable) {
                    verifyImmutableTs(
                            timestamp, immutableTs, prePunch, lastFreshTs, tss.getFreshTimestamp(), wallClockTimestamp);
                } else {
                    verifyFreshTs(timestamp, prePunch, lastFreshTs, tss.getFreshTimestamp(), wallClockTimestamp);
                }
            } catch (RuntimeException e) {
                throw new RuntimeException("Failed " + e.getMessage() + " while processing result:\n" + output, e);
            }
        });
    }

    @Test
    public void testWithFile() throws Exception {
        String inputFileString = "test.timestamp";
        runAndVerifyCliForFile(inputFileString);
    }

    @Test
    public void testWithFileWithDir() throws Exception {
        String inputFileString = "existing-dir/test.timestamp";
        runAndVerifyCliForFile(inputFileString);
    }

    @Test
    public void testWithFileWithMissingDir() throws Exception {
        String inputFileString = "missing-dir/test.timestamp";
        runAndVerifyCliForFile(inputFileString);
    }

    @Test
    public void testWithFileWithInvalidMissingDir() throws Exception {
        String inputFileString = "readonly-dir/missing-dir/test.timestamp";
        File inputFile = new File(inputFileString);
        File readOnlyDir = inputFile.getParentFile().getParentFile();
        Files.createDirectory(readOnlyDir.toPath());
        assertThat(readOnlyDir.setReadOnly()).isTrue();
        assertThatThrownBy(() -> runAndVerifyCliForFile(inputFileString))
                .isInstanceOf(RuntimeException.class)
                .hasCauseExactlyInstanceOf(AccessDeniedException.class);
    }

    private void runAndVerifyCliForFile(String inputFileString) throws Exception {
        cliArgs.add("-f");
        cliArgs.add(inputFileString);
        cliArgs.add("fetch");
        if (isImmutable) {
            cliArgs.add("-i");
        }
        cliArgs.add("-d");
        runAndVerifyCli((runner, tss, immutableTs, prePunch, lastFreshTs, shouldTestImmutable) -> {
            String output = runner.run(true, false);
            try {
                Scanner scanner = new Scanner(output);
                String shouldBeDeprecationLine = scanner.nextLine();
                verifyDeprecation(shouldBeDeprecationLine);
                long timestamp = getTimestampFromFile(inputFileString);
                scanner.nextLine();
                long wallClockTimestamp = getWallClockTimestamp(scanner);
                scanner.close();
                if (shouldTestImmutable && isImmutable) {
                    verifyImmutableTs(
                            timestamp, immutableTs, prePunch, lastFreshTs, tss.getFreshTimestamp(), wallClockTimestamp);
                } else {
                    verifyFreshTs(timestamp, prePunch, lastFreshTs, tss.getFreshTimestamp(), wallClockTimestamp);
                }
            } catch (RuntimeException e) {
                throw new RuntimeException("Failed " + e.getMessage() + " while verifying result:\n" + output, e);
            }
        });
    }

    private void runAndVerifyCli(Verifier verifier) throws Exception {
        try (SingleBackendCliTestRunner runner = makeRunner(cliArgs.toArray(new String[0]))) {
            TestAtlasDbServices services = (TestAtlasDbServices) runner.connect(moduleFactory);
            LockService lockService = services.getLockService();
            TimestampService tss = services.getManagedTimestampService();
            LockClient client = services.getTestLockClient();

            Clock clock = GlobalClock.create(lockService);
            long prePunch = clock.getTimeMillis();
            punch(services, tss, clock);

            long immutableTs = tss.getFreshTimestamp();
            LockRequest request = LockRequest.builder(ImmutableSortedMap.of(lock, LockMode.WRITE))
                    .withLockedInVersionId(immutableTs)
                    .doNotBlock()
                    .build();
            LockRefreshToken token = lockService.lock(client.getClientId(), request);
            long lastFreshTs = tss.getFreshTimestamps(1000).getUpperBound();

            verifier.verify(runner, tss, immutableTs, prePunch, lastFreshTs, true);

            lockService.unlock(token);
            lastFreshTs = tss.getFreshTimestamps(1000).getUpperBound();

            // there are no locks so we now expect immutable to just be a fresh
            runner.freshCommand();
            verifier.verify(runner, tss, immutableTs, prePunch, lastFreshTs, false);
        }
    }

    private void punch(TestAtlasDbServices services, TimestampService tss, Clock clock) {
        // this is a really hacky way of forcing a punch to test the datetime output
        Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(2));
        long punchTs = tss.getFreshTimestamps(1000).getUpperBound();
        PuncherStore puncherStore = KeyValueServicePuncherStore.create(services.getKeyValueService());
        Puncher puncher = SimplePuncher.create(
                puncherStore, clock, Suppliers.ofInstance(AtlasDbConstants.DEFAULT_TRANSACTION_READ_TIMEOUT));
        puncher.punch(punchTs);
        Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(2));
    }

    private long getWallClockTimestamp(Scanner scanner) {
        String line = scanner.findInLine(".*Wall\\sclock\\sdatetime.*\\s(\\d+.*)");
        List<String> parts = Splitter.on(' ').splitToList(line);
        return ISODateTimeFormat.dateTime()
                .parseDateTime(parts.get(parts.size() - 1))
                .getMillis();
    }

    private long getTimestampFromStdout(Scanner scanner) {
        String line = scanner.findInLine(".*timestamp\\sis\\:\\s(\\d+.*)");
        List<String> parts = Splitter.on(' ').splitToList(line);
        return Long.parseLong(parts.get(parts.size() - 1));
    }

    private long getTimestampFromFile(String fileString) throws IOException {
        File file = new File(fileString);
        assertThat(file).exists();
        List<String> lines = Files.readAllLines(file.toPath(), StandardCharsets.UTF_8);
        return Long.parseLong(lines.get(0));
    }

    private void verifyFreshTs(
            long timestamp, long prePunch, long lastFreshTs, long newFreshTs, long wallClockTimestamp) {
        assertThat(wallClockTimestamp).isGreaterThan(prePunch);
        assertThat(timestamp).isGreaterThan(lastFreshTs);
        assertThat(timestamp).isLessThan(newFreshTs);
    }

    private void verifyImmutableTs(
            long timestamp,
            long immutableTs,
            long prePunch,
            long lastFreshTs,
            long newFreshTs,
            long wallClockTimestamp) {
        assertThat(wallClockTimestamp).isGreaterThan(prePunch);
        assertThat(timestamp).isEqualTo(immutableTs);
        assertThat(timestamp).isLessThan(lastFreshTs);
        assertThat(timestamp).isLessThan(newFreshTs);
    }

    private void verifyDeprecation(String shouldBeDeprecationLine) {
        assertThat(shouldBeDeprecationLine).contains("This CLI has been deprecated.");
    }
}
