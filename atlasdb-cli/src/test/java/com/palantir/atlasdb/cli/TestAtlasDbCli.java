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
package com.palantir.atlasdb.cli;

import java.net.URISyntaxException;
import java.nio.file.Paths;

import org.junit.BeforeClass;
import org.junit.Test;
import org.kohsuke.args4j.Option;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cli.api.AbstractAtlasDbCli;
import com.palantir.atlasdb.cli.api.AtlasDbServices;

public class TestAtlasDbCli extends AbstractAtlasDbCli {

    private static String SIMPLE_CONFIG_FILE;
    private static String NESTED_CONFIG_FILE;

    @Option(name = "--flag1", aliases = { "-f1" })
    Boolean flag1;

    @Option(name = "--flag2", aliases = { "-f2" })
    String flag2;

    public TestAtlasDbCli() {
        super(SIMPLE_CONFIG_FILE);
    }

    @Override
    public int execute(AtlasDbServices services) {
        try {
            // test a method on each of the services
            if (flag1 != null) {
                services.getKeyValueService().getAllTableNames();
                services.getTimestampService().getFreshTimestamp();
                services.getLockSerivce().getMinLockedInVersionId("test-client");
                services.getTransactionManager().getImmutableTimestamp();
            }

            // test kvs create table
            if (flag2 != null) {
                services.getKeyValueService().createTable(flag2, AtlasDbConstants.GENERIC_TABLE_METADATA);
                Preconditions.checkArgument(services.getKeyValueService().getAllTableNames().contains(flag2),
                        "kvs contains tables %s, but not table %s", services.getKeyValueService().getAllTableNames(), flag2);
                services.getKeyValueService().dropTable(flag2);
            }
            return 0;
        } finally {
            services.getKeyValueService().teardown();
        }
    }

    @BeforeClass
    public static void setup() throws URISyntaxException {
        SIMPLE_CONFIG_FILE = Paths.get(TestAtlasDbCli.class.getClassLoader().getResource("simple_atlasdb_config.yml").toURI()).toString();
        NESTED_CONFIG_FILE = Paths.get(TestAtlasDbCli.class.getClassLoader().getResource("nested_atlasdb_config.yml").toURI()).toString();
    }

    @Test
    public void testFailure() {
        assertFailure(new TestAtlasDbCli().run(new String[] { "-noopt" }));
    }

    @Test
    public void testRunHelp() {
        assertSuccessful(new TestAtlasDbCli().run(new String[] { "--help" }));
    }

    @Test
    public void testRun() {
        assertSuccessful(new TestAtlasDbCli().run(new String[] { "--config", SIMPLE_CONFIG_FILE}));
    }

    @Test
    public void testFlag1Run() {
        assertSuccessful(new TestAtlasDbCli().run(new String[] { "--config", SIMPLE_CONFIG_FILE, "--flag1"}));
    }

    @Test
    public void testFlag2Run() {
        assertSuccessful(new TestAtlasDbCli().run(new String[] { "-c", SIMPLE_CONFIG_FILE, "--flag2", "test.new_table"}));
    }

    @Test
    public void testRunNestedConfig() {
        assertSuccessful(new TestAtlasDbCli().run(new String[] { "-c", NESTED_CONFIG_FILE, "-f1", "-n", "config", "dropwizardConfig", "-f2", "test.new_table"}));
    }

    @Test
    public void testDefaultConfig() {
        assertSuccessful(new TestAtlasDbCli().run(new String[] { }));
    }

    private void assertSuccessful(int returnVal) {
        Preconditions.checkArgument(returnVal == 0, "CLI exited with non-zero exit code.");
    }

    private void assertFailure(int returnVal) {
        Preconditions.checkArgument(returnVal == 1, "CLI exited with exit code zero.");
    }

}
