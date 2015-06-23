package com.palantir.atlasdb.shell;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.impl.ShellAwareReadOnlyTransactionManager;
import com.palantir.atlasdb.transaction.impl.SnapshotTransactionManager;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockService;
import com.palantir.timestamp.TimestampService;

/**
 * The {@link AtlasShellConnectionFactory} factory, as you may have guessed, knows how to make
 * {@link AtlasShellConnection}s.
 */
public final class AtlasShellConnectionFactory {
    private final AtlasShellContextFactory atlasShellContextFactory;

    public AtlasShellConnectionFactory(AtlasShellContextFactory atlasShellContextFactory) {
        this.atlasShellContextFactory = atlasShellContextFactory;
    }

    /**
     * Create an {@link AtlasShellConnection} with a {@link SnapshotTransactionManager} in it
     */
    public AtlasShellConnection withSnapshotTransactionManager(KeyValueService keyValueService,
                                                               TransactionService transactionService,
                                                               LockClient lockClient,
                                                               LockService lockService,
                                                               TimestampService timestampService) {
        AtlasContext atlasContext = atlasShellContextFactory.withSnapshotTransactionManager(
                keyValueService,
                transactionService,
                lockClient,
                lockService,
                timestampService);
        return AtlasShellConnection.createAtlasShellConnection(atlasContext);
    }

    /**
     * Create an {@link AtlasShellConnection} with a {@link ShellAwareReadOnlyTransactionManager} in
     * it
     */
    public AtlasShellConnection withShellAwareReadOnlyTransactionManager(KeyValueService keyValueService,
                                                                         TransactionService transactionService) {
        AtlasContext atlasContext = atlasShellContextFactory.withShellAwareReadOnlyTransactionManager(
                keyValueService,
                transactionService);
        return AtlasShellConnection.createAtlasShellConnection(atlasContext);
    }

    /**
     * Create an {@link AtlasShellConnection} with a {@link SnapshotTransactionManager} in it,
     * implemented entirely in memory and with blank initial state
     */
    public AtlasShellConnection withSnapshotTransactionManagerInMemory() {
        AtlasContext atlasContext = atlasShellContextFactory.withSnapshotTransactionManagerInMemory();
        return AtlasShellConnection.createAtlasShellConnection(atlasContext);
    }

    /**
     * Create an {@link AtlasShellConnection} with a {@link ShellAwareReadOnlyTransactionManager} in
     * it, implemented entirely in memory and with blank initial state
     */
    public AtlasShellConnection withShellAwareReadOnlyTransactionManagerInMemory() {
        AtlasContext atlasContext = atlasShellContextFactory.withShellAwareReadOnlyTransactionManagerInMemory();
        return AtlasShellConnection.createAtlasShellConnection(atlasContext);
    }

    /**
     * Create an {@link AtlasShellConnection} with a {@link SnapshotTransactionManager} in it by
     * connecting to a dispatch server
     *
     * @param host host to connect to
     * @param port port to connect to
     * @param user user name to log in with
     * @param pass password to log in with
     * @return an {@link AtlasShellConnection}
     */
    public AtlasShellConnection withSnapshotTransactionManagerFromDispatch(String host,
                                                                           String port,
                                                                           String user,
                                                                           String pass) {
        AtlasContext atlasContext = atlasShellContextFactory.withSnapshotTransactionManagerFromDispatch(
                host,
                port,
                user,
                pass);
        return AtlasShellConnection.createAtlasShellConnection(atlasContext);
    }

    public AtlasShellConnection withSnapshotTransactionManagerFromDispatchPrefs() {
        AtlasContext atlasContext = atlasShellContextFactory.withSnapshotTransactionManagerFromDispatchPrefs();
        return AtlasShellConnection.createAtlasShellConnection(atlasContext);
    }

    public AtlasShellConnection withShellAwareReadOnlyTransactionManagerFromDb(String host,
                                                                               String port,
                                                                               String sid,
                                                                               String type,
                                                                               String username,
                                                                               String password) {
        AtlasContext atlasContext = atlasShellContextFactory.withShellAwareReadOnlyTransactionManagerFromDb(
                host,
                port,
                sid,
                type,
                username,
                password);
        return AtlasShellConnection.createAtlasShellConnection(atlasContext);
    }
}
