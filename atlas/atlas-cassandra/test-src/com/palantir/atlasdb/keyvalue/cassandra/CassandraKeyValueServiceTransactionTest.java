package com.palantir.atlasdb.keyvalue.cassandra;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.impl.AbstractTransactionTest;

public class CassandraKeyValueServiceTransactionTest extends AbstractTransactionTest {

    @Override
    protected KeyValueService getKeyValueService() {
        return CassandraKeyValueService.create(
                ImmutableSet.of("localhost"),
                9160,
                20,
                "atlasdb",
                false,
                1,
                10000,
                10000000,
                1000,
                false,
                false,
                null);
    }

    @Override
    protected boolean supportsReverse() {
        return false;
    }

}
