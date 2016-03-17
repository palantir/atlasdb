package com.palantir.atlasdb.cli.runner;

import java.io.IOException;

import org.apache.commons.io.FileUtils;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.cli.command.SingleBackendCommand;
import com.palantir.atlasdb.rocksdb.RocksDbKeyValueServiceConfig;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.common.base.Throwables;

public class RocksDbTestRunner extends AbstractTestRunner {

    public static final String SIMPLE_ROCKSDB_CONFIG_FILENAME = "simple_rocksdb_config.yml";

    public RocksDbTestRunner(Class<? extends SingleBackendCommand> cmdClass, String... args) {
        super(cmdClass, args);
    }

    @Override
    protected String getKvsConfigFileName() {
        return SIMPLE_ROCKSDB_CONFIG_FILENAME;
    }

    @Override
    protected void cleanup(KeyValueServiceConfig kvsConfig) {
        Preconditions.checkArgument(kvsConfig instanceof RocksDbKeyValueServiceConfig,
                "RocksDbAtlasDbFactory expects a configuration of type RocksDbKeyValueServiceConfig, found %s", kvsConfig.getClass());
        RocksDbKeyValueServiceConfig rocksDbConfig = (RocksDbKeyValueServiceConfig) kvsConfig;
        try {
            FileUtils.deleteDirectory(rocksDbConfig.dataDir());
        } catch (IOException e) {
            Throwables.rewrapAndThrowUncheckedException("Could not cleanup after cli test", e);
        }
    }

}
