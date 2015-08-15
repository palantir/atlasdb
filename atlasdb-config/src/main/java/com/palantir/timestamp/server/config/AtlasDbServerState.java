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
package com.palantir.timestamp.server.config;

import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.palantir.atlasdb.client.FailoverFeignTarget;
import com.palantir.atlasdb.client.TextDelegateDecoder;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.memory.InMemoryAtlasDb;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.timestamp.TimestampService;
import com.palantir.timestamp.server.config.AtlasDbServerConfiguration.ServerType;

import feign.Client;
import feign.Feign;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import feign.jaxrs.JAXRSContract;

public class AtlasDbServerState {
    final KeyValueService keyValueService;
    final Supplier<TimestampService> timestampSupplier;
    final SerializableTransactionManager transactionManager;

    public AtlasDbServerState(KeyValueService keyValueService,
                              Supplier<TimestampService> timestampSupplier,
                              SerializableTransactionManager transactionManager) {
        this.keyValueService = keyValueService;
        this.timestampSupplier = timestampSupplier;
        this.transactionManager = transactionManager;
    }

    public KeyValueService getKeyValueService() {
        return keyValueService;
    }

    public Supplier<TimestampService> getTimestampSupplier() {
        return timestampSupplier;
    }

    public SerializableTransactionManager getTransactionManager() {
        return transactionManager;
    }

    public static AtlasDbServerState create(AtlasDbServerConfiguration config) {
        RemoteLockService leadingLock;
        if (config.lockClient.local) {
            leadingLock = LockServiceImpl.create();
        } else {
            leadingLock = getServiceWithFailover(config.lockClient.servers, RemoteLockService.class);
        }
        TimestampService leadingTs = getServiceWithFailover(config.timestampClient.servers, TimestampService.class);

        if (config.serverType == ServerType.LEVELDB) {
            Preconditions.checkArgument(config.leader.leaders.size() == 1, "only one server allowed for LevelDB");
            if (config.timestampClient.local) {
                return LevelDbAtlasServerFactory.createWithLocalTimestampService(config.levelDbDir, new Schema(), leadingLock);
            } else {
                return LevelDbAtlasServerFactory.create(config.levelDbDir, new Schema(), leadingTs, leadingLock);
            }
        } else if (config.serverType == ServerType.MEMORY) {
            Preconditions.checkArgument(config.leader.leaders.size() == 1, "only one server allowed for MEMORY");
            SerializableTransactionManager txnMgr = InMemoryAtlasDb.createInMemoryTransactionManager(new Schema());
            return new AtlasDbServerState(txnMgr.getKeyValueService(), Suppliers.ofInstance(txnMgr.getTimestampService()), txnMgr);
        } else {
            if (config.timestampClient.local) {
                return CassandraAtlasServerFactory.createWithLocalTimestampService(config.cassandra, new Schema(), leadingLock);
            } else {
                return CassandraAtlasServerFactory.create(config.cassandra, new Schema(), leadingTs, leadingLock);
            }
        }
    }

    private static <T> T getServiceWithFailover(List<String> uris, Class<T> type) {
        ObjectMapper mapper = new ObjectMapper();
        FailoverFeignTarget<T> failoverFeignTarget = new FailoverFeignTarget<T>(uris, type);
        Client client = failoverFeignTarget.wrapClient(new Client.Default(null, null));
        return Feign.builder()
                .decoder(new TextDelegateDecoder(new JacksonDecoder(mapper)))
                .encoder(new JacksonEncoder(mapper))
                .contract(new JAXRSContract())
                .client(client)
                .retryer(failoverFeignTarget)
                .target(failoverFeignTarget);
    }
}