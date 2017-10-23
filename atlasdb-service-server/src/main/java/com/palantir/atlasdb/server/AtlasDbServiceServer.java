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
package com.palantir.atlasdb.server;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientFactory;
import com.palantir.atlasdb.server.generated.TodoSchemaTableFactory;
import com.palantir.atlasdb.server.generated.TodoTable;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.exception.NotInitializedException;
import com.palantir.remoting3.servers.jersey.HttpRemotingJerseyFeature;
import com.palantir.tritium.metrics.MetricRegistries;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class AtlasDbServiceServer extends Application<AtlasDbServiceServerConfiguration> {
    private static SerializableTransactionManager tm;

    public static void main(String[] args) throws Exception {
        new AtlasDbServiceServer().run(args);
        runTxns();
    }

    @Override
    public void initialize(Bootstrap<AtlasDbServiceServerConfiguration> bootstrap) {
        super.initialize(bootstrap);
        bootstrap.getObjectMapper().registerModule(new Jdk8Module());
        bootstrap.setMetricRegistry(MetricRegistries.createWithHdrHistogramReservoirs());
    }

    @Override
    public void run(AtlasDbServiceServerConfiguration config, final Environment environment) throws Exception {
        AtlasDbMetrics.setMetricRegistry(environment.metrics());
        environment.jersey().register(HttpRemotingJerseyFeature.INSTANCE);
        Schema schema = TodoSchema.getSchema();
        schema.renderTables(new File("src/main/java/schema"));

        tm = TransactionManagers.builder()
                .config(config.getConfig())
                .registrar(environment.jersey()::register)
                .schemas(ImmutableSet.of(TodoSchema.getSchema()))
                .buildSerializable();


//        TableMetadataCache cache = new TableMetadataCache(tm.getKeyValueService());

//        environment.jersey().register(new AtlasDbServiceImpl(tm.getKeyValueService(), tm, cache));
//        environment.getObjectMapper().registerModule(new AtlasJacksonModule(cache).createModule());
    }

    private static void runTxns() {
        while (true) {
            try {
                tm.runTaskWithRetry(txn -> {
                    TodoSchemaTableFactory tableFactory = TodoSchemaTableFactory.of();
                    TodoTable todoTable = tableFactory.getTodoTable(txn);
                    todoTable.putText(TodoTable.TodoRow.of(7), "yeah");
                    todoTable.putText(TodoTable.TodoRow.of(1), "nopes");
                    return null;
                });

                List<TodoTable.TodoRowResult> todoRowResults = tm.runTaskWithRetry(txn -> {
                    TodoSchemaTableFactory tableFactory = TodoSchemaTableFactory.of();
                    TodoTable todoTable = tableFactory.getTodoTable(txn);
                    return todoTable.getRows(ImmutableList.of(TodoTable.TodoRow.of(7), TodoTable.TodoRow.of(1)));
                });

                System.out.println(todoRowResults);

                tm.runTaskWithRetry(
                        txn -> {
                            TodoSchemaTableFactory tableFactory = TodoSchemaTableFactory.of();
                            TodoTable todoTable = tableFactory.getTodoTable(txn);
                            todoTable.delete(ImmutableList.of(TodoTable.TodoRow.of(7), TodoTable.TodoRow.of(1)));
                            return null;
                        }
                );
                Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
            } catch (NotInitializedException e) {
                System.out.println("looks like the tm is not initialized yet.");
//            } catch (PalantirRuntimeException e) { //ClientCreationFailedException
//                if (e.getMessage().contains("Failed to construct client for ")) {
//                    System.out.println("looks like the tm failed to construct client. " + e.getMessage());
//                } else {
//                    System.out.println("oohhhhh no" + e.getMessage());
//                }
            } catch (CassandraClientFactory.ClientCreationFailedException e) {
                System.out.println("looks like the tm throws something." + e.getMessage());
//            } catch (Throwable t) {
//                System.out.println("looks like the tm throws something." + t.getMessage());
            }
        }
    }

}
