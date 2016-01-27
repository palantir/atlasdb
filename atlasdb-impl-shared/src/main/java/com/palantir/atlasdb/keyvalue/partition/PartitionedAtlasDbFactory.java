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
package com.palantir.atlasdb.keyvalue.partition;

import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ExecutorService;

import com.google.auto.service.AutoService;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.partition.endpoint.InMemoryKeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.endpoint.KeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.map.DynamicPartitionMapImpl;
import com.palantir.atlasdb.keyvalue.partition.map.InMemoryPartitionMapService;
import com.palantir.atlasdb.keyvalue.partition.map.PartitionMapService;
import com.palantir.atlasdb.keyvalue.partition.quorum.QuorumParameters;
import com.palantir.atlasdb.keyvalue.remoting.RemotingKeyValueService;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.spi.TimestampServiceConfig;
import com.palantir.atlasdb.spi.TransactionServiceConfig;
import com.palantir.atlasdb.timestamp.config.PaxosTimestampServiceConfig;
import com.palantir.atlasdb.transaction.config.PaxosTransactionServiceConfig;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.timestamp.TimestampService;
import com.palantir.util.crypto.Sha256Hash;

@AutoService(AtlasDbFactory.class)
public class PartitionedAtlasDbFactory implements AtlasDbFactory {

    @Override
    public String getType() {
        return "partitioned";
    }

    @Override
    public PartitionedKeyValueService createRawKeyValueService(KeyValueServiceConfig kvc) {
        if (kvc instanceof StaticPartitionedKeyValueConfiguration) {
            StaticPartitionedKeyValueConfiguration config = (StaticPartitionedKeyValueConfiguration) kvc;
            List<String> endpoints = config.getKeyValueEndpoints();
            QuorumParameters quorum = new QuorumParameters(endpoints.size(), (endpoints.size()+1)/2, endpoints.size()/2 + 1);
            ExecutorService exec = PTExecutors.newCachedThreadPool();
            NavigableMap<byte[], KeyValueEndpoint> ring = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());
            for (String url : endpoints) {
                KeyValueService kv = RemotingKeyValueService.createClientSide(url, Suppliers.ofInstance(0L));
                InMemoryPartitionMapService mapService = InMemoryPartitionMapService.createEmpty();
                KeyValueEndpoint endpoint = InMemoryKeyValueEndpoint.create(kv, mapService);
//                KeyValueEndpoint endpoint = SimpleKeyValueEndpoint.create(url, url);
                ring.put(Sha256Hash.computeHash(url.getBytes(Charsets.UTF_8)).getBytes(), endpoint);
            }
            DynamicPartitionMapImpl paritionMap = DynamicPartitionMapImpl.create(quorum, ring, exec);
            List<PartitionMapService> mapServices = Lists.newArrayList();
            for (KeyValueEndpoint endpoint : ring.values()) {
                mapServices.add(endpoint.partitionMapService());
                endpoint.partitionMapService().updateMap(paritionMap);
            }
            return PartitionedKeyValueService.create(quorum, mapServices);
        }
        PartitionedKeyValueConfiguration config = (PartitionedKeyValueConfiguration) kvc;
        return PartitionedKeyValueService.create(config);
    }

    @Override
    public TimestampService createTimestampService(Optional<TimestampServiceConfig> config,
                                                   KeyValueService rawKvs) {
        Preconditions.checkArgument(config.isPresent() && config.get() instanceof PaxosTimestampServiceConfig);
        return TransactionServices.createTimestampService((PaxosTimestampServiceConfig) config.get());
    }

    @Override
    public TransactionService createTransactionService(Optional<TransactionServiceConfig> config, KeyValueService rawKvs) {
        Preconditions.checkArgument(config.isPresent() && config.get() instanceof PaxosTransactionServiceConfig);
        return TransactionServices.createTransactionService(config, rawKvs);
    }

}
