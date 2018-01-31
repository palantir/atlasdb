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

import java.util.Optional;

import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cli.output.OutputPrinter;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServiceImpl;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;

import io.airlift.airline.Command;

@Command(name = "clean-cass-locks-state", description = "Clean up and get the schema mutation "
        + "locks for the CassandraKVS into a good state")
public class CleanCassLocksStateCommand extends AbstractCommand {
    private static final OutputPrinter printer = new OutputPrinter(
            LoggerFactory.getLogger(CleanCassLocksStateCommand.class));

    @Override
    public Integer call() throws Exception {
        Preconditions.checkState(isOffline(), "This CLI can only be run offline");

        CassandraKeyValueServiceConfig config = getCassandraKvsConfig();
        return runWithConfig(config, getAtlasDbConfig().initializeAsync());
    }

    @VisibleForTesting
    public Integer runWithConfig(CassandraKeyValueServiceConfig config, boolean initializeAsync) throws Exception {
        CassandraKeyValueService ckvs = CassandraKeyValueServiceImpl.create(
                config,
                Optional.empty(),
                initializeAsync);

        ckvs.cleanUpSchemaMutationLockTablesState();
        printer.info("Schema mutation lock cli completed successfully.");
        return 0;
    }

    private CassandraKeyValueServiceConfig getCassandraKvsConfig() {
        KeyValueServiceConfig kvsConfig = getAtlasDbConfig().keyValueService();
        if (!kvsConfig.type().equals(CassandraKeyValueServiceConfig.TYPE)) {
            throw new IllegalStateException(
                    String.format("KeyValueService must be of type %s, but yours is %s",
                            CassandraKeyValueServiceConfig.TYPE, kvsConfig.type()));
        }
        return (CassandraKeyValueServiceConfig) kvsConfig;
    }

}
