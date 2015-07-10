package com.palantir.atlasdb.keyvalue.cassandra;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
    CassandraKeyValueServiceSerializableTransactionTest.class/*,
    CassandraKeyValueServiceTransactionTest.class,
    CassandraTimestampTest.class,
    CQLKeyValueServiceSerializableTransactionTest.class,
    CQLKeyValueServiceTransactionTest.class*/
})
public class CassandraTestSuite {

    @BeforeClass
    public static void setup() throws IOException, InterruptedException {
        System.setProperty("cassandra-foreground", "true");
        System.setProperty("cassandra.boot_without_jna", "false");
        CassandraService.start();
    }

    @AfterClass
    public static void stop() throws IOException {
        CassandraService.stop();
    }

}
