/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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

import static org.junit.Assert.assertFalse;

import java.net.URISyntaxException;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ObjectArrays;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cli.runner.AbstractTestRunner;
import com.palantir.atlasdb.cli.runner.InMemoryTestRunner;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.schema.SweepSchema;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.atlasdb.table.description.Schemas;

public class TestKvsMigrationCommand {
    private KvsMigrationCommand getCommand(String[] args) throws URISyntaxException {
        String filePath = AbstractTestRunner.getResourcePath(InMemoryTestRunner.CONFIG_LOCATION);
        String[] initArgs = new String[] { "migrate", "-fc", filePath, "-mc", filePath };
        String[] fullArgs = ObjectArrays.concat(initArgs, args, String.class);
        return AbstractTestRunner.buildCommand(KvsMigrationCommand.class, fullArgs);
    }

    @Test
    public void doesNotSweepDuringMigration() throws Exception {
        KvsMigrationCommand cmd = getCommand(new String[] { "-smv" });
        AtlasDbServices fromServices = cmd.connectFromServices();
        assertFalse(fromServices.getAtlasDbRuntimeConfig().sweep().enabled());

        AtlasDbServices toServices = cmd.connectToServices();
        assertFalse(toServices.getAtlasDbRuntimeConfig().sweep().enabled());
    }

    @Test
    public void canMigrateZeroTables() throws Exception {
        runTestWithTableSpecs(0, 0);
    }

    @Test
    public void canMigrateOneTable() throws Exception {
        runTestWithTableSpecs(1, 1);
    }

    @Test
    public void canMigrateMultipleTables() throws Exception {
        runTestWithTableSpecs(10, 257);
    }

    private void runTestWithTableSpecs(int numTables, int numEntriesPerTable) throws Exception {
        KvsMigrationCommand cmd = getCommand(new String[] { "-smv" });
        AtlasDbServices fromServices = cmd.connectFromServices();

        // CLIs don't currently reinitialize the KVS
        Schemas.createTablesAndIndexes(SweepSchema.INSTANCE.getLatestSchema(), fromServices.getKeyValueService());
        AtlasDbServices toServices = cmd.connectToServices();
        seedKvs(fromServices, numTables, numEntriesPerTable);
        try {
            int exitCode = cmd.execute(fromServices, toServices);
            Assert.assertEquals(0, exitCode);
            checkKvs(toServices, numTables, numEntriesPerTable);
        } finally {
            fromServices.close();
            toServices.close();
        }
    }

    private void seedKvs(AtlasDbServices services, int numTables, int numEntriesPerTable) {
        for (int i = 0; i < numTables; i++) {
            TableReference table = TableReference.create(Namespace.create("ns"), "table" + i);
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
            TableReference table = TableReference.create(Namespace.create("ns"), "table" + i);
            services.getKeyValueService().createTable(table, AtlasDbConstants.GENERIC_TABLE_METADATA);
            services.getTransactionManager().runTaskThrowOnConflict(t -> {
                ImmutableSet.Builder<Cell> toRead = ImmutableSet.builder();
                ImmutableMap.Builder<Cell, byte[]> expectedBuilder = ImmutableMap.builder();
                for (int j = 0; j < numEntriesPerTable; j++) {
                    Cell cell = Cell.create(PtBytes.toBytes("row" + j), PtBytes.toBytes("col"));
                    toRead.add(cell);
                    expectedBuilder.put(cell, PtBytes.toBytes("val" + j));
                }
                Map<Cell, byte[]> expected = expectedBuilder.build();
                Map<Cell, byte[]> result = t.get(table, expected.keySet());
                Assert.assertEquals(expected.keySet(), result.keySet());
                for (Entry<Cell, byte[]> e : result.entrySet()) {
                    Assert.assertArrayEquals(expected.get(e.getKey()), e.getValue());
                }
                return null;
            });
        }
    }
}
