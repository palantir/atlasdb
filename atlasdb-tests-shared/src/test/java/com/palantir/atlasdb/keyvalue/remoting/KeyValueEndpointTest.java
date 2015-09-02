package com.palantir.atlasdb.keyvalue.remoting;

import static org.junit.Assert.assertEquals;
import io.dropwizard.testing.junit.DropwizardClientRule;

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

public class KeyValueEndpointTest extends AbstractAtlasDbKeyValueServiceTest {

	@Rule
	public final DropwizardClientRule endpointKvsService = Utils.getRemoteKvsRule(
			RemotingKeyValueService.createServerSide(new InMemoryKeyValueService(false), Suppliers.ofInstance(-1L)));

	@Rule
	public final DropwizardClientRule endpointPmsService = new DropwizardClientRule(
			Preconditions.checkNotNull(new PartitionMapService() {
				long version = 0L;

				@Override
				public void updateMap(DynamicPartitionMap partitionMap) {
					version = partitionMap.getVersion();
				}

				@Override
				public long getMapVersion() {
					return version;
				}

				@Override
				public DynamicPartitionMap getMap() {
					return null;
				}
			}));

    private KeyValueEndpoint endpoint;

    @Before
    public void setupPrivate() {
        Utils.setupRuleHacks(endpointKvsService);
        Utils.setupRuleHacks(endpointPmsService);
        endpoint = new SimpleKeyValueEndpoint(endpointKvsService.baseUri().toString(), endpointPmsService.baseUri().toString());
        endpoint.build(Suppliers.ofInstance(-1L));
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
