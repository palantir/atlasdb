package com.palantir.atlasdb.keyvalue.cassandra;

import org.junit.Ignore;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.impl.AbstractSerializableTransactionTest;

public class CQLKeyValueServiceSerializableTransactionTest extends
        AbstractSerializableTransactionTest {

    @Override
    protected KeyValueService getKeyValueService() {
        return CQLKeyValueService.create(
                ImmutableSet.of("localhost"),
                9160,
                20,
                1000,
                "atlasdb",
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

    @Override
    @Ignore
    public void testRangePaging() {
    }

}
