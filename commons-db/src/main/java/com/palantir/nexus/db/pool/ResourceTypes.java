package com.palantir.nexus.db.pool;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;

import com.google.common.reflect.AbstractInvocationHandler;
import com.palantir.nexus.db.DBType;
import com.palantir.proxy.util.ProxyUtils;

public final class ResourceTypes {
    private ResourceTypes() {
        // no
    }

    public static final ResourceType<ConnectionManager, SQLException> CONNECTION_MANAGER = new ResourceType<ConnectionManager, SQLException>() {
        @Override
        public void close(ConnectionManager r) throws SQLException {
            r.close();
        }

        @Override
        public ConnectionManager closeWrapper(final ConnectionManager delegate, final ResourceOnClose<SQLException> onClose) {
            return new BaseConnectionManager() {
                @Override
                public Connection getConnection() throws SQLException {
                    return delegate.getConnection();
                }

                @Override
                public void close() throws SQLException {
                    onClose.close();
                }

                @Override
                public void init() throws SQLException {
                    delegate.init();
                }

                @Override
                public DBType getDbType() {
                    return delegate.getDbType();
                }
            };
        }

        @Override
        public String name() {
            return "ConnectionManager";
        }
    };

    public static final ResourceType<Connection, SQLException> CONNECTION = new ResourceType<Connection, SQLException>() {
        @Override
        public void close(Connection r) throws SQLException {
            r.close();
        }

        @Override
        public Connection closeWrapper(final Connection delegate, final ResourceOnClose<SQLException> onClose) {
            InvocationHandler ih = new AbstractInvocationHandler() {
                @Override
                protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
                    if (args.length == 0 && method.getName().equals("close")) {
                        onClose.close();
                        return null;
                    }
                    try {
                        return method.invoke(delegate, args);
                    } catch (InvocationTargetException e) {
                        throw e.getTargetException();
                    }
                }
            };
            return ProxyUtils.newProxy(Connection.class, Object.class, ih);
        }

        @Override
        public String name() {
            return "Connection";
        }
    };
}
