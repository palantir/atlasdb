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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.Maps;

public class ColumnFamilyMap {
    public static class ColumnFamily implements AutoCloseable {
        private final long index;
        private final ColumnFamilyHandle handle;
        private final AtomicInteger refCount = new AtomicInteger();

        public ColumnFamily(long index, ColumnFamilyHandle handle) {
            this.index = index;
            this.handle = handle;
        }

        public ColumnFamilyHandle getHandle() {
            return handle;
        }

        @Override
        public void close() {
            refCount.decrementAndGet();
        }
    }
    private final Map<String, ColumnFamily> cfs = Maps.newConcurrentMap();
    private final Function<String, ColumnFamilyDescriptor> cfFactory;
    private final RocksDB db;

    public ColumnFamilyMap(Function<String, ColumnFamilyDescriptor> cfFactory,
                           RocksDB db) {
        this.cfFactory = cfFactory;
        this.db = db;
    }

    public void initialize(List<ColumnFamilyDescriptor> cfDescriptors,
                           List<ColumnFamilyHandle> cfHandles) throws RocksDBException {
        for (int i = 0; i < cfDescriptors.size(); i++) {
            String fullTableName = new String(cfDescriptors.get(i).columnFamilyName(), Charsets.UTF_8);
            int nameIndex = fullTableName.lastIndexOf("__");
            String tableName;
            long index;
            if (nameIndex == -1) {
                tableName = fullTableName;
                index = 0;
            } else {
                tableName = fullTableName.substring(0, nameIndex);
                index = Long.parseLong(fullTableName.substring(nameIndex + 1));
            }
            ColumnFamily cf = new ColumnFamily(index, cfHandles.get(i));
            ColumnFamily oldCf = cfs.put(tableName, cf);
            if (oldCf != null && !tableName.equals("default")) {
                db.dropColumnFamily(oldCf.getHandle());
                oldCf.getHandle().dispose();
            }
        }
    }

    public Set<String> getTableNames() {
        return cfs.keySet();
    }

    public ColumnFamily get(String tableName) {
        ColumnFamily cf = cfs.get(tableName);
        if (cf == null) {
            throw new IllegalArgumentException("Table " + tableName + " does not exist.");
        }
        cf.refCount.incrementAndGet();
        return cf;
    }

    public synchronized void create(String tableName) throws RocksDBException {
        ColumnFamily cf = cfs.get(tableName);
        if (cf == null) {
            ColumnFamilyDescriptor descriptor = cfFactory.apply(tableName);
            ColumnFamilyHandle handle = db.createColumnFamily(descriptor);
            cfs.put(tableName, new ColumnFamily(0, handle));
        }
    }

    public synchronized void drop(String tableName) throws RocksDBException {
        ColumnFamily cf = cfs.remove(tableName);
        if (cf != null) {
            db.dropColumnFamily(cf.handle);
            cf.handle.dispose();
        }
    }

    public synchronized void truncate(String tableName) throws InterruptedException, RocksDBException {
        ColumnFamily oldCf = cfs.get(tableName);
        if (oldCf == null) {
            throw new IllegalArgumentException("Table " + tableName + " does not exist.");
        }
        long newIndex = (oldCf.index + 1) % 2;
        String realTableName = String.format("%s__%d", tableName, newIndex);
        ColumnFamilyDescriptor descriptor = cfFactory.apply(realTableName);
        ColumnFamilyHandle handle = db.createColumnFamily(descriptor);
        cfs.put(tableName, new ColumnFamily(newIndex, handle));
        while (oldCf.refCount.get() > 0) {
            Thread.sleep(10);
        }
        db.dropColumnFamily(oldCf.handle);
        oldCf.handle.dispose();
    }
}
