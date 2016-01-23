package com.palantir.atlasdb.jdbc;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.palantir.atlasdb.keyvalue.jdbc.JdbcKeyValueService;
import com.palantir.atlasdb.keyvalue.jdbc.JdbcTimestampBoundStore;

public class JdbcTimestampBoundStoreTest {
    private JdbcKeyValueService kvs;
    private JdbcTimestampBoundStore store;

    @Before
    public void setUp() throws Exception {
        kvs = JdbcTests.createEmptyKvs();
        store = JdbcTimestampBoundStore.create(kvs);
    }

    @After
    public void tearDown() throws Exception {
        kvs.close();
    }

    @Test
    public void testTimestampBoundStore() {
        long upperLimit1 = store.getUpperLimit();
        long upperLimit2 = store.getUpperLimit();
        Assert.assertEquals(upperLimit1, upperLimit2);
        store.storeUpperLimit(upperLimit2 + 1);
        long upperLimit3 = store.getUpperLimit();
        Assert.assertEquals(upperLimit3, upperLimit2 + 1);
    }
}
