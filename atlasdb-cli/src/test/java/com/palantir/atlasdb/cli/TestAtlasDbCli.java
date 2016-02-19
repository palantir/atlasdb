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

import com.google.common.base.Preconditions;
import com.lexicalscope.jewel.cli.Option;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cli.api.AbstractAtlasDbCli;
import com.palantir.atlasdb.cli.api.AtlasDbCliOptions;
import com.palantir.atlasdb.cli.api.AtlasDbServices;

public class TestAtlasDbCli extends AbstractAtlasDbCli<TestAtlasDbCli.TestAtlasDbCliOptions> {

    private static String TEST_CONFIG_FILE;

    @Override
    public int execute(AtlasDbServices services, TestAtlasDbCliOptions opts) {
        try {
            // test a method on each of the services
            if (opts.getFlag1()) {
                services.getKeyValueService().getAllTableNames();
                services.getTimestampService().getFreshTimestamp();
                services.getLockSerivce().getMinLockedInVersionId("test-client");
                services.getTransactionManager().getImmutableTimestamp();
            }

            // test kvs create table
            if (opts.isFlag2()) {
                services.getKeyValueService().createTable(opts.getFlag2(), AtlasDbConstants.GENERIC_TABLE_METADATA);
                Preconditions.checkArgument(services.getKeyValueService().getAllTableNames().contains(opts.getFlag2()),
                        "kvs contains tables %s, but not table %s", services.getKeyValueService().getAllTableNames(), opts.getFlag2());
                services.getKeyValueService().dropTable(opts.getFlag2());
            }
            return 0;
        } finally {
            services.getKeyValueService().teardown();
        }
    }

    public interface TestAtlasDbCliOptions extends AtlasDbCliOptions {
        @Option(longName = "required", shortName = "r")
        boolean getRequired();

        @Option(longName = "flag1", shortName = "f1", defaultValue = "false")
        boolean getFlag1();

        @Option(longName = "flag2", shortName = "f2")
        String getFlag2();
        boolean isFlag2();
    }

    @BeforeClass
    public static void setup() throws URISyntaxException {
        TEST_CONFIG_FILE = Paths.get(TestAtlasDbCli.class.getClassLoader().getResource("test_atlasdb_config.yml").toURI()).toString();
    }

    @Test
    public void testRun() {
        assertSuccessful(new TestAtlasDbCli().run(new String[] { "--config", TEST_CONFIG_FILE, "-r"}, TestAtlasDbCliOptions.class));
    }

    @Test
    public void testFlag1Run() {
        assertSuccessful(new TestAtlasDbCli().run(new String[] { "--config", TEST_CONFIG_FILE, "--flag1"}, TestAtlasDbCliOptions.class));
    }

    @Test
    public void testFlag2Run() {
        assertSuccessful(new TestAtlasDbCli().run(new String[] { "-c", TEST_CONFIG_FILE, "--flag2", "test.new_table"}, TestAtlasDbCliOptions.class));
    }

    private void assertSuccessful(int returnVal) {
        Preconditions.checkArgument(returnVal == 0, "CLI exited with non-zero exit code.");
    }

}
