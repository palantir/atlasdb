/**
 * Copyright 2015 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
        for (String tableName : cfs.getTableNames()) {
            if (cfs.getTableNames().contains(tableName)) {
                forceCompaction(tableName);
            }
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
        StringBuilder s = new StringBuilder();
        for (String tableName : cfs.getTableNames()) {
            if (cfs.getTableNames().contains(tableName)) {
                s.append(tableName).append(" : ").append(getProperty(tableName, property)).append("\n");
            }
        }
        return s.toString();
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
