package com.palantir.atlasdb.keyvalue.remoting;

import org.junit.Before;
import org.junit.Rule;

import com.palantir.atlasdb.keyvalue.impl.partition.AbstractPartitionMapServiceTest;
import com.palantir.atlasdb.keyvalue.partition.PartitionMapService;
import com.palantir.atlasdb.keyvalue.partition.PartitionMapServiceImpl;
import com.palantir.atlasdb.keyvalue.partition.api.PartitionMap;
import com.palantir.atlasdb.keyvalue.partition.util.VersionedObject;

import io.dropwizard.testing.junit.DropwizardClientRule;

public class PartitionMapServiceRemotingTest extends AbstractPartitionMapServiceTest {

    private final PartitionMapService remoteService = new PartitionMapServiceImpl(samplePartitionMap, initialVersion);

    @Rule
    public final DropwizardClientRule rule = new DropwizardClientRule(remoteService);

    private PartitionMapService localService;

    @Before
    public void setUp() {
        Utils.setupRuleHacks(rule);
        localService = RemotingPartitionMapService.createClientSide(rule.baseUri().toString());
    }

    @Override
    protected PartitionMapService getPartitionMapService(VersionedObject<PartitionMap> partitionMap) {
        return localService;
    }

}
