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
package com.palantir.atlasdb.keyvalue.remoting;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.NavigableMap;

import org.apache.commons.lang.ArrayUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.partition.api.DynamicPartitionMap;
import com.palantir.atlasdb.keyvalue.partition.endpoint.InMemoryKeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.endpoint.KeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.map.DynamicPartitionMapImpl;
import com.palantir.atlasdb.keyvalue.partition.map.InMemoryPartitionMapService;
import com.palantir.atlasdb.keyvalue.partition.map.PartitionMapService;
import com.palantir.atlasdb.keyvalue.partition.quorum.QuorumParameters;
import com.palantir.atlasdb.keyvalue.partition.server.EndpointServer;
import com.palantir.atlasdb.keyvalue.remoting.outofband.InboxPopulatingContainerRequestFilter;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.PTExecutors;

import io.dropwizard.Configuration;
import io.dropwizard.testing.DropwizardTestSupport;
import io.dropwizard.testing.junit.DropwizardClientRule;

public class Utils {

    public static final SimpleModule module = RemotingKeyValueService.kvsModule();
    public static final ObjectMapper mapper = RemotingKeyValueService.kvsMapper();

    public static DropwizardClientRule getRemoteKvsRule(KeyValueService remoteKvs) {
        DropwizardClientRule rule = new DropwizardClientRule(remoteKvs,
                KeyAlreadyExistsExceptionMapper.instance(),
                InsufficientConsistencyExceptionMapper.instance(),
                ClientVersionTooOldExceptionMapper.instance(),
                EndpointVersionTooOldExceptionMapper.instance(),
                new InboxPopulatingContainerRequestFilter(mapper));
        return rule;
    }

    public static void setupRuleHacks(DropwizardClientRule rule) {
        try {
            Field field = rule.getClass().getDeclaredField("testSupport");
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            DropwizardTestSupport<Configuration> testSupport = (DropwizardTestSupport<Configuration>) field.get(rule);
            ObjectMapper mapper = testSupport.getEnvironment().getObjectMapper();
            mapper.registerModule(Utils.module);
            mapper.registerModule(new GuavaModule());
            testSupport.getApplication();
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    public static class RemoteKvs {
        public final KeyValueService delegate;
        public final KeyValueService remoteKvs;
        public final DropwizardClientRule rule;

        public RemoteKvs(KeyValueService delegate, final RemotePms remotePms) {
            this.delegate = delegate;
            remoteKvs = RemotingKeyValueService.createServerSide(delegate, new Supplier<Long>() {
                @Override
                public Long get() {
                    Long version = RemotingPartitionMapService.createClientSide(remotePms.rule.baseUri().toString()).getMapVersion();
                    return version;
                }
            });
            rule = Utils.getRemoteKvsRule(remoteKvs);
        }
    }

    public static class RemotePms {
        public final PartitionMapService service;
        public final DropwizardClientRule rule;
        public RemotePms(PartitionMapService pms) {
            this.service = pms;
            this.rule = new DropwizardClientRule(pms);
        }
    }

    public static class RemoteEndpoint {
        final EndpointServer server;
        final public RemotePms pms;
        final public RemoteKvs kvs;

        public RemoteEndpoint(KeyValueService kvsDelegate, PartitionMapService pmsDelegate) {
            this.server = new EndpointServer(kvsDelegate, pmsDelegate);
            this.pms = new RemotePms(server);
            this.kvs = new RemoteKvs(server, this.pms);
        }
    }

}