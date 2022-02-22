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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ObjectArrays;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cli.runner.AbstractTestRunner;
import com.palantir.atlasdb.cli.runner.InMemoryTestRunner;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.services.AtlasDbServices;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.Callable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestKvsMigrationCommand {
    private AtlasDbServices fromServices;
    private AtlasDbServices toServices;

    @Before
    public void setupServices() throws Exception {
        KvsMigrationCommand cmd = getCommand(new String[] {});
        fromServices = cmd.connectFromServices();
        toServices = cmd.connectToServices();
    }

    @After
    public void close() {
        fromServices.close();
        toServices.close();
    }

    @Test
    public void doesNotSweepDuringMigration() {
        assertThat(fromServices.getAtlasDbRuntimeConfig().sweep().enabled()).contains(false);
        assertThat(fromServices.getAtlasDbRuntimeConfig().targetedSweep().enabled())
                .isFalse();

        assertThat(toServices.getAtlasDbRuntimeConfig().sweep().enabled()).contains(false);
        assertThat(toServices.getAtlasDbRuntimeConfig().targetedSweep().enabled())
                .isFalse();
    }

    @Test
    public void canRunMultipleTasksAtOnce() throws Exception {
        assertSuccess(() -> runWithOptions("--setup", "--migrate"));
        assertSuccess(() -> runWithOptions("--setup", "--validate"));
        assertSuccess(() -> runWithOptions("--migrate", "--validate"));
        assertSuccess(() -> runWithOptions("--setup", "--migrate", "--validate"));
    }

    @Test
    public void canMigrateZeroTables() throws Exception {
        assertSuccess(() -> runWithOptions("--setup"));
        assertSuccess(() -> runWithOptions("--migrate"));
        assertSuccess(() -> runWithOptions("--validate"));
    }

    @Test
    public void canMigrateOneTable() throws Exception {
        seedKvs(fromServices, 1, 1);
        assertSuccess(() -> runWithOptions("--setup"));
        assertSuccess(() -> runWithOptions("--migrate"));
        assertSuccess(() -> runWithOptions("--validate"));
        checkKvs(toServices, 1, 1);
    }

    @Test
    public void canMigrateMultipleTables() throws Exception {
        seedKvs(fromServices, 10, 257);
        assertSuccess(() -> runWithOptions("--setup"));
        assertSuccess(() -> runWithOptions("--migrate"));
        assertSuccess(() -> runWithOptions("--validate"));
        checkKvs(toServices, 10, 257);
    }

    private KvsMigrationCommand getCommand(String[] args) throws URISyntaxException {
        String filePath = AbstractTestRunner.getResourcePath(InMemoryTestRunner.CONFIG_LOCATION);
        String[] initArgs = new String[] {"migrate", "-fc", filePath, "-mc", filePath};
        String[] fullArgs = ObjectArrays.concat(initArgs, args, String.class);
        return AbstractTestRunner.buildCommand(KvsMigrationCommand.class, fullArgs);
    }

    private void assertSuccess(Callable<Integer> callable) throws Exception {
        assertThat(callable.call()).isEqualTo(0);
    }

    private int runWithOptions(String... args) throws URISyntaxException {
        KvsMigrationCommand command = getCommand(args);
        return command.execute(fromServices, toServices);
    }

    private void seedKvs(AtlasDbServices services, int numTables, int numEntriesPerTable) {
        for (int i = 0; i < numTables; i++) {
            TableReference table = getTableRef(i);
            services.getKeyValueService().createTable(table, AtlasDbConstants.GENERIC_TABLE_METADATA);
            services.getTransactionManager().runTaskThrowOnConflict(t -> {
                ImmutableMap.Builder<Cell, byte[]> toWrite = ImmutableMap.builder();
                for (int j = 0; j < numEntriesPerTable; j++) {
                    Cell cell = Cell.create(PtBytes.toBytes("row" + j), PtBytes.toBytes("col"));
                    toWrite.put(cell, PtBytes.toBytes("val" + j));
                }
                t.put(table, toWrite.build());
                return null;
            });
        }
    }

    private void checkKvs(AtlasDbServices services, int numTables, int numEntriesPerTable) {
        for (int i = 0; i < numTables; i++) {
            TableReference table = getTableRef(i);
            services.getTransactionManager().runTaskThrowOnConflict(t -> {
                ImmutableMap.Builder<Cell, byte[]> expectedBuilder = ImmutableMap.builder();
                for (int j = 0; j < numEntriesPerTable; j++) {
                    Cell cell = Cell.create(PtBytes.toBytes("row" + j), PtBytes.toBytes("col"));
                    expectedBuilder.put(cell, PtBytes.toBytes("val" + j));
                }
                Map<Cell, byte[]> expected = expectedBuilder.build();
                Map<Cell, byte[]> result = t.get(table, expected.keySet());
                assertThat(result.keySet()).containsExactlyInAnyOrderElementsOf(expected.keySet());
                for (Map.Entry<Cell, byte[]> e : result.entrySet()) {
                    assertThat(e.getValue()).isEqualTo(expected.get(e.getKey()));
                }
                return null;
            });
        }
    }

    private static TableReference getTableRef(int number) {
        return TableReference.create(Namespace.create("ns"), "table" + number);
    }
}
