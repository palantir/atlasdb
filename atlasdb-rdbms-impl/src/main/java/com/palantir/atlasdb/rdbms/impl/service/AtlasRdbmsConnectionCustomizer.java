package com.palantir.atlasdb.rdbms.impl.service;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

import javax.sql.DataSource;

import org.apache.commons.dbutils.QueryRunner;

import com.google.common.collect.Lists;
import com.google.common.reflect.AbstractInvocationHandler;
import com.palantir.atlasdb.rdbms.impl.util.TempTableDescriptor;
import com.palantir.util.StringUtils;

public final class AtlasRdbmsConnectionCustomizer {

    private final Collection<String> tempTableSql;

    private AtlasRdbmsConnectionCustomizer(Collection<String> tempTableSql) {
        this.tempTableSql = tempTableSql;
    }

    static AtlasRdbmsConnectionCustomizer create(Collection<TempTableDescriptor> tempTables) {
        Collection<String> tempTableSql = Lists.newArrayList();
        for (TempTableDescriptor descriptor : tempTables) {
            List<String> cols = Lists.newArrayList();
            for (Entry<String, String> entry : descriptor.getColumns().entrySet()) {
                cols.add(entry.getKey() + " " + entry.getValue());
            }
            tempTableSql.add(
                    "CREATE LOCAL TEMPORARY TABLE " + descriptor.getTableName() + "(" + StringUtils.join(cols, ", ") +
                    ", PRIMARY KEY (" + StringUtils.join(descriptor.getColumns().keySet(), ", ")  + ")) ON COMMIT DELETE ROWS");
        }
        return new AtlasRdbmsConnectionCustomizer(tempTableSql);
    }

    public static DataSource customize(final DataSource delegate, Collection<TempTableDescriptor> tempTables) {
        final AtlasRdbmsConnectionCustomizer customizer = create(tempTables);
        return (DataSource) Proxy.newProxyInstance(delegate.getClass().getClassLoader(), new Class[] { DataSource.class },
                new AbstractInvocationHandler() {
            @Override
            public Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
                if (method.getName().equals("getConnection")) {
                    Connection conn = (args.length == 0) ? delegate.getConnection() :
                        delegate.getConnection((String) args[0], (String) args[1]);
                    customizer.onAcquire(conn);
                    return conn;
                }
                return method.invoke(delegate, args);
            }
        });
    }

    public void onAcquire(Connection connection) {
        try {
            QueryRunner qr = new QueryRunner();
            for (String sql : tempTableSql) {
                qr.update(connection, sql);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}