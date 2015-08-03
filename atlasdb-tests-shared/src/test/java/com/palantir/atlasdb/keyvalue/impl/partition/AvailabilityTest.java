package com.palantir.atlasdb.keyvalue.impl.partition;

import java.util.Random;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.AbstractAtlasDbKeyValueServiceTest;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.FailableKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.PartitionedKeyValueService;
import com.palantir.atlasdb.keyvalue.partition.QuorumParameters;

public class AvailabilityTest extends AbstractAtlasDbKeyValueServiceTest {

    KeyValueService kvs;
    public static final int NUM_SERVICES = 5;
    static final FailableKeyValueService[] services = new FailableKeyValueService[NUM_SERVICES];
    QuorumParameters quorumParameters = new QuorumParameters(5, 3, 3);
    // TODO: Make it deterministic (?)
    Random random = new Random();

    @Override
    protected KeyValueService getKeyValueService() {
        if (kvs == null) {
            createServices();
        }
        return kvs;
    }

    private Set<Integer> generateBrokenServices() {
        return generateBrokenServices(
                Sets.<Integer> newHashSet(),
                quorumParameters.getReplicationFactor() - quorumParameters.getReadFactor());
    }

    private Set<Integer> generateBrokenServices(Set<Integer> brokenReads, int numBreaks) {
        if (brokenReads.size() == numBreaks) {
            return brokenReads;
        }
        brokenReads.add(random.nextInt(NUM_SERVICES));
        return generateBrokenServices(brokenReads, numBreaks);
    }

    private void createServices() {
        for (int i = 0; i < NUM_SERVICES; ++i) {
            FailableKeyValueService service = FailableKeyValueService.wrap(new InMemoryKeyValueService(
                    false));
            services[i] = service;
        }
        Set<? extends Integer> brokenServices = generateBrokenServices();
        for (Integer i : brokenServices) {
            services[i].setBrokenRead(true);
            services[i].setBrokenWrite(true);
        }
        kvs = PartitionedKeyValueService.create(
                ImmutableSet.<FailableKeyValueService> copyOf(services),
                new QuorumParameters(5, 3, 3));
    }

}
