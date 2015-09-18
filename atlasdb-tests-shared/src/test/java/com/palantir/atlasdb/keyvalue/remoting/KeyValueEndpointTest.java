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

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.AbstractAtlasDbKeyValueServiceTest;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.api.DynamicPartitionMap;
import com.palantir.atlasdb.keyvalue.partition.endpoint.KeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.endpoint.SimpleKeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.map.PartitionMapService;

import io.dropwizard.testing.junit.DropwizardClientRule;

public class KeyValueEndpointTest extends AbstractAtlasDbKeyValueServiceTest {

    @Rule
    public final DropwizardClientRule endpointKvsService = Utils.getRemoteKvsRule(
            RemotingKeyValueService.createServerSide(new InMemoryKeyValueService(false), Suppliers.ofInstance(-1L)));

    @Rule
    public final DropwizardClientRule endpointPmsService = new DropwizardClientRule(
            Preconditions.checkNotNull(new PartitionMapService() {
                long version = 0L;

                @Override
                public synchronized void updateMap(DynamicPartitionMap partitionMap) {
                    version = partitionMap.getVersion();
                }

                @Override
                public synchronized long updateMapIfNewer(DynamicPartitionMap partitionMap) {
                    version = Math.max(version, partitionMap.getVersion());
                    return version;
                }

                @Override
                public synchronized long getMapVersion() {
                    return version;
                }

                @Override
                public synchronized DynamicPartitionMap getMap() {
                    return null;
                }
            }));

    private KeyValueEndpoint endpoint;

    @Before
    public void setupPrivate() {
        Utils.setupRuleHacks(endpointKvsService);
        Utils.setupRuleHacks(endpointPmsService);
        endpoint = new SimpleKeyValueEndpoint(endpointKvsService.baseUri().toString(), endpointPmsService.baseUri().toString());
        endpoint.registerPartitionMapVersion(Suppliers.ofInstance(-1L));
    }

    private KeyValueEndpoint getEndpoint() {
        setupPrivate();
        return Preconditions.checkNotNull(endpoint);
    }

    @Test
    public void testSimple() {
        assertEquals(0L, getEndpoint().partitionMapService().getMapVersion());
    }

    @Override
    protected KeyValueService getKeyValueService() {
        setupPrivate();
        return Preconditions.checkNotNull(getEndpoint().keyValueService());
    }

}
