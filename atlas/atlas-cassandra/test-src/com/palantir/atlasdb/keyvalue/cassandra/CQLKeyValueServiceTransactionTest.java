package com.palantir.atlasdb.keyvalue.cassandra;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.impl.AbstractTransactionTest;

public class CQLKeyValueServiceTransactionTest extends AbstractTransactionTest {

    @Override
    protected KeyValueService getKeyValueService() {
        return CQLKeyValueService.create(
                ImmutableSet.of("localhost"),
                9160,
                1000,
                1000,
                "atlas",
                false,
                1,
                1000,
                10000000,
                1000,
                true,
                false,
                null);
    }

    @Override
    protected boolean supportsReverse() {
        return false;
    }

}
