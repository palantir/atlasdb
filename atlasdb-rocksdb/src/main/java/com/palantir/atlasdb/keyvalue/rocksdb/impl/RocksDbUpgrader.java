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

import java.io.File;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.rocksdb.impl.ColumnFamilyMap.ColumnFamily;
import com.palantir.atlasdb.rocksdb.RocksDbAtlasDbFactory;
import com.palantir.atlasdb.rocksdb.RocksDbKeyValueServiceConfig;

public class RocksDbUpgrader {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            help();
            return;
        }

        RocksDbKeyValueServiceConfig oldConfig = MAPPER.readValue(new File(args[0]), RocksDbKeyValueServiceConfig.class);
        RocksDbKeyValueServiceConfig newConfig = MAPPER.readValue(new File(args[1]), RocksDbKeyValueServiceConfig.class);

        if (!oldConfig.dataDir().exists()) {
            System.err.println("Old config data directory " + oldConfig.dataDir().getAbsolutePath() + " does not exist.");
            return;
        }
        if (newConfig.dataDir().exists() && newConfig.dataDir().list().length > 0) {
            System.err.println("New config data directory " + newConfig.dataDir().getAbsolutePath() + " already exists and is non-empty.");
            return;
        }

        RocksDbAtlasDbFactory factory = new RocksDbAtlasDbFactory();
        RocksDbKeyValueService oldKvs = factory.createRawKeyValueService(oldConfig);
        RocksDbKeyValueService newKvs = factory.createRawKeyValueService(newConfig);

        for (String table : oldKvs.getAllTableNames()) {
            newKvs.createTable(table, AtlasDbConstants.EMPTY_TABLE_METADATA);
        }
        factory.createTimestampService(newKvs);

        for (String table : oldKvs.columnFamilies.getTableNames()) {
            System.out.println("Migrating table " + table);
            ColumnFamily oldCf = oldKvs.columnFamilies.get(table);
            ColumnFamily newCf = newKvs.columnFamilies.get(table);
            ColumnFamilyHandle newHandle = newCf.getHandle();
            WriteOptions opts = new WriteOptions().setDisableWAL(true).setSync(false);
            RocksIterator iter = oldKvs.db.newIterator(oldCf.getHandle());
            iter.seekToFirst();
            long count = 0;
            while (iter.isValid()) {
                byte[] key = iter.key();
                byte[] value = iter.value();
                newKvs.db.put(newHandle, opts, key, value);
                iter.next();
                if (++count % 10000 == 0) {
                    System.out.println("Migrated " + count + " rows...");
                }
            }
            System.out.println("Finished migrating " + count + " rows from " + table + ".");
        }
        System.out.println("Flushing all writes...");
        oldKvs.close();
        newKvs.close();
        System.out.println("Finished migration of all tables.");
    }

    private static void help() {
        System.out.println("RocksDbUpgrader");
        System.out.println();
        System.out.println("  Use this cli to copy your existing rocksdb database");
        System.out.println("  into a new database with possibly different settings.");
        System.out.println();
        System.out.println("Usage: RocksDbUpgrader old-settings.json new-settings.json");
        System.out.println();
        System.out.println("  old-settings.json");
        System.out.println("    Configuration for your existing rocksdb database. In");
        System.out.println("    java this deserializes as a KeyValueServiceConfig");
        System.out.println("    object (specifically a RocksDbKeyValueServiceConfig).");
        System.out.println("    This must contain the entry");
        System.out.println("      \"type\": \"rocksdb\"");
        System.out.println("    as well as a \"dataDir\" key specifying the location of");
        System.out.println("    your data.");
        System.out.println();
        System.out.println("  new-settings.json");
        System.out.println("    Configuration for your new rocksdb database. The");
        System.out.println("    format is the same as for old-settings.json");
    }
}
