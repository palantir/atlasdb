package com.palantir.atlasdb.shell;


public interface AtlasShellContextFactory {
    AtlasContext withReadOnlyTransactionManagerCassandra(String host, String port, String keyspace);

    AtlasContext withSnapshotTransactionManagerInMemory();
}
