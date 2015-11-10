package com.palantir.atlasdb.keyvalue.rocksdb.impl;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import com.google.common.base.Throwables;
import com.palantir.atlasdb.keyvalue.rocksdb.impl.ColumnFamilyMap.ColumnFamily;

public class RocksDbMXBeanImpl implements RocksDbMXBean {
    private final RocksDB db;
    private final ColumnFamilyMap cfs;

    public RocksDbMXBeanImpl(RocksDB db, ColumnFamilyMap cfs) {
        this.db = db;
        this.cfs = cfs;
    }

    @Override
    public String[] listTableNames() {
        return cfs.getTableNames().toArray(new String[0]);
    }

    @Override
    public void forceCompaction() {
        try {
            db.compactRange();
        } catch (RocksDBException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void forceCompaction(String tableName) {
        try (ColumnFamily cf = cfs.get(tableName)) {
            if (cf == null) {
                throw new IllegalArgumentException("Unknown table name: " + tableName);
            }
            db.compactRange(cf.getHandle());
        } catch (RocksDBException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public String getProperty(String property) {
        try {
            return db.getProperty(property);
        } catch (RocksDBException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public String getProperty(String tableName, String property) {
        try (ColumnFamily cf = cfs.get(tableName)) {
            if (cf == null) {
                throw new IllegalArgumentException("Unknown table name: " + tableName);
            }
            return db.getProperty(cf.getHandle(), property);
        } catch (RocksDBException e) {
            throw Throwables.propagate(e);
        }
    }
}
