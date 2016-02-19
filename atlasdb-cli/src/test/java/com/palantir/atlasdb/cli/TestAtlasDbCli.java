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
        Preconditions.checkArgument(new TestAtlasDbCli().run(new String[] { "--config", TEST_CONFIG_FILE, "-r"}, TestAtlasDbCliOptions.class) == 0);
    }

    @Test
    public void testFlag1Run() {
        Preconditions.checkArgument(new TestAtlasDbCli().run(new String[] { "--config", TEST_CONFIG_FILE, "--flag1"}, TestAtlasDbCliOptions.class) == 0);
    }

    @Test
    public void testFlag2Run() {
        Preconditions.checkArgument(new TestAtlasDbCli().run(new String[] { "-c", TEST_CONFIG_FILE, "--flag2", "test.new_table"}, TestAtlasDbCliOptions.class) == 0);
    }

}
