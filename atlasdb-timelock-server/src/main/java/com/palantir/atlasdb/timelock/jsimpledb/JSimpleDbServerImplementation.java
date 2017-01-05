/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.timelock.jsimpledb;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.dellroad.stuff.net.TCPNetwork;
import org.jsimpledb.JSimpleDB;
import org.jsimpledb.JSimpleDBFactory;
import org.jsimpledb.core.Database;
import org.jsimpledb.kv.RetryTransactionException;
import org.jsimpledb.kv.mvcc.AtomicKVDatabase;
import org.jsimpledb.kv.raft.RaftKVDatabase;
import org.jsimpledb.kv.raft.RaftKVTransaction;
import org.jsimpledb.kv.sqlite.SQLiteKVDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.atlasdb.timelock.ServerImplementation;
import com.palantir.atlasdb.timelock.TimeLockServices;
import com.palantir.atlasdb.timelock.config.TimeLockServerConfiguration;
import com.palantir.lock.impl.LockServiceImpl;

public class JSimpleDbServerImplementation implements ServerImplementation {
    private static final Logger log = LoggerFactory.getLogger(JSimpleDbServerImplementation.class);
    private JSimpleDB jdb;
    private RaftKVDatabase raft;
    private SQLiteKVDatabase sqlite;

    @Override
    public void onStart(TimeLockServerConfiguration configuration) {
        sqlite = new SQLiteKVDatabase();
        sqlite.setDatabaseFile(new File("var/data/timestamps.sqlite"));

        raft = new RaftKVDatabase();
        raft.setLogDirectory(new File(configuration.atomix().storageDirectory()));
        raft.setKVStore(new AtomicKVDatabase(sqlite));
        raft.setIdentity(configuration.cluster().localServer().host() + ":" + configuration.cluster().localServer().port());
        raft.setNetwork(new TCPNetwork(configuration.cluster().localServer().port()));
        raft.start();

        List<String> serversToAdd = Ordering.natural()
                .sortedCopy(configuration.cluster().servers().stream()
                        .map(addr -> String.format("%s:%s", addr.host(), addr.port()))
                        .collect(Collectors.toList()));
        while (serversToAdd.size() != raft.getCurrentConfig().size()) {
            log.info("Our current config: {}", raft.getCurrentConfig());
            if (raft.getIdentity().equals(Iterables.getFirst(serversToAdd, null))) {
                serversToAdd.forEach(addr -> {
                    try {
                        RaftKVTransaction tx = raft.createTransaction();
                        tx.configChange(addr, addr);
                        tx.commit();
                    } catch (RetryTransactionException e) {
                        log.info("Waiting for node at {}", addr, e);
                    }
                });
            } else {
                log.info("Waiting to be added to the cluster");
            }
            if (serversToAdd.size() != raft.getCurrentConfig().size()) {
                Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
            }
        }

        jdb = new JSimpleDBFactory()
                .setDatabase(new Database(raft))
                .setSchemaVersion(1)
                .setModelClasses(Timestamp.class)
                .newJSimpleDB();
    }

    @Override
    public void onStop() {
        if (jdb != null) {
            raft.stop();
            sqlite.stop();
            raft = null;
            sqlite = null;
            jdb = null;
        }
    }

    @Override
    public void onFail() {
        onStop();
    }

    @Override
    public TimeLockServices createInvalidatingTimeLockServices(String client) {
        return JSimpleDbInvalidatingLeaderProxy.create(
                raft,
                () -> TimeLockServices.create(
                        new JSimpleDbTimestampService(jdb, client),
                        LockServiceImpl.create()),
                TimeLockServices.class);
    }
}
