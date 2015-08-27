package com.palantir.atlasdb.keyvalue.remoting;

import java.lang.reflect.Field;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.AbstractAtlasDbKeyValueServiceTest;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.KeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.PartitionMapService;
import com.palantir.atlasdb.keyvalue.partition.PartitionMapServiceImpl;
import com.palantir.atlasdb.keyvalue.partition.QuorumParameters;
import com.palantir.atlasdb.keyvalue.partition.SimpleKeyValueEndpoint;

import io.dropwizard.Configuration;
import io.dropwizard.testing.DropwizardTestSupport;
import io.dropwizard.testing.junit.DropwizardClientRule;

public class KeyValueEndpointTest extends AbstractAtlasDbKeyValueServiceTest {

    static final KeyValueService endpointKvs = RemotingKeyValueService.createServerSide(new InMemoryKeyValueService(false), null);
    static final PartitionMapService endpointPms = new PartitionMapServiceImpl();
    static final QuorumParameters QUORUM_PARAMETERS = new QuorumParameters(3, 2, 2);

    private final SimpleModule module = RemotingKeyValueService.kvsModule();
    private final ObjectMapper mapper = RemotingKeyValueService.kvsMapper();

    @Rule public final DropwizardClientRule endpointKvsService = new DropwizardClientRule(endpointKvs);
    @Rule public final DropwizardClientRule endpointPmsService = new DropwizardClientRule(endpointPms);

    KeyValueEndpoint endpoint;

    @SuppressWarnings("unchecked")
    @Before
    public void setupHacks() throws Exception {
        Field field = endpointKvsService.getClass().getDeclaredField("testSupport");
        field.setAccessible(true);
        DropwizardTestSupport<Configuration> testSupport = (DropwizardTestSupport<Configuration>) field.get(endpointKvsService);
        ObjectMapper mapper = testSupport.getEnvironment().getObjectMapper();
        mapper.registerModule(module);
        mapper.registerModule(new GuavaModule());
        testSupport.getApplication();
    }

    @Test
    public void testSimple() {
    }

    @Override
    protected KeyValueService getKeyValueService() {
        if (endpoint == null) {
            endpoint = new SimpleKeyValueEndpoint(endpointKvsService.baseUri().toString(), endpointPmsService.baseUri().toString());
        }
        return Preconditions.checkNotNull(endpoint.keyValueService());
    }

}
