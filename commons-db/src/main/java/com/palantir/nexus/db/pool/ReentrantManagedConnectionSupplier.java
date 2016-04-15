package com.palantir.nexus.db.pool;

import java.sql.Connection;
import java.sql.SQLException;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Preconditions;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.ReentrantConnectionSupplier;

@ThreadSafe
public class ReentrantManagedConnectionSupplier implements ReentrantConnectionSupplier {

    private final ConnectionManager delegate;
    private final ThreadLocal<ResourceSharer<Connection, SQLException>> threadLocal;

    public ReentrantManagedConnectionSupplier(final ConnectionManager delegate) {
        this.delegate = Preconditions.checkNotNull(delegate);
        this.threadLocal = new ThreadLocal<ResourceSharer<Connection, SQLException>>() {
            @Override
            protected ResourceSharer<Connection, SQLException> initialValue() {
                return new ResourceSharer<Connection, SQLException>(ResourceTypes.CONNECTION) {
                    @Override
                    public Connection open() {
                        return delegate.getConnectionUnchecked();
                    }
                };
            }
        };
    }

    @Override
    public Connection get() throws PalantirSqlException {
        return CloseTracking.wrap(threadLocal.get().get());
    }

    @Override
    public Connection getUnsharedConnection() throws PalantirSqlException {
        return CloseTracking.wrap(delegate.getConnectionUnchecked());
    }

    @Override
    public void close() throws PalantirSqlException {
        delegate.closeUnchecked();
    }
}
