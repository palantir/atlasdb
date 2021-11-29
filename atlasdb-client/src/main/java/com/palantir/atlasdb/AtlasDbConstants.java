/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TargetedSweepMetadata;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.LogSafety;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;

public final class AtlasDbConstants {
    private AtlasDbConstants() {
        // Utility
    }

    public static final TableReference PUNCH_TABLE = TableReference.createWithEmptyNamespace("_punch");
    public static final TableReference OLD_SCRUB_TABLE = TableReference.createWithEmptyNamespace("_scrub");
    public static final TableReference SCRUB_TABLE = TableReference.createWithEmptyNamespace("_scrub2");
    public static final TableReference NAMESPACE_TABLE = TableReference.createWithEmptyNamespace("_namespace");
    public static final TableReference TIMESTAMP_TABLE = TableReference.createWithEmptyNamespace("_timestamp");
    public static final TableReference SWEEP_PROGRESS_TABLE =
            TableReference.createWithEmptyNamespace("_sweep_progress3");
    public static final TableReference LEGACY_TIMELOCK_TIMESTAMP_TABLE =
            TableReference.createWithEmptyNamespace("pt_metropolis_ts");
    public static final TableReference DB_TIMELOCK_TIMESTAMP_TABLE =
            TableReference.createWithEmptyNamespace("pt_timelock_db_ts");
    public static final TableReference PERSISTED_LOCKS_TABLE =
            TableReference.createWithEmptyNamespace("_persisted_locks");
    public static final TableReference COORDINATION_TABLE = TableReference.createWithEmptyNamespace("_coordination");

    public static final TableReference DEFAULT_METADATA_TABLE = TableReference.createWithEmptyNamespace("_metadata");
    public static final TableReference DEFAULT_ORACLE_METADATA_TABLE =
            TableReference.createWithEmptyNamespace("atlasdb_metadata");
    public static final TableReference DEFAULT_SCHEMA_METADATA_TABLE =
            TableReference.createWithEmptyNamespace("_schema_metadata");

    // Deprecated tables
    public static final TableReference SWEEP_PROGRESS_V1 =
            TableReference.createFromFullyQualifiedName("sweep.progress");
    public static final TableReference SWEEP_PROGRESS_V1_5 =
            TableReference.createWithEmptyNamespace("_sweep_progress1_5");
    public static final TableReference SWEEP_PROGRESS_V2 = TableReference.createWithEmptyNamespace("_sweep_progress2");
    public static final String LOCK_TABLE_PREFIX = "_locks";

    public static final ImmutableSet<TableReference> DEPRECATED_SWEEP_TABLES_WITH_NO_METADATA =
            ImmutableSet.of(SWEEP_PROGRESS_V1, SWEEP_PROGRESS_V1_5, SWEEP_PROGRESS_V2);

    public static final String PRIMARY_KEY_CONSTRAINT_PREFIX = "pk_";

    private static final int ORACLE_NAME_LENGTH_LIMIT = 30;
    public static final int ATLASDB_ORACLE_TABLE_NAME_LIMIT =
            AtlasDbConstants.ORACLE_NAME_LENGTH_LIMIT - PRIMARY_KEY_CONSTRAINT_PREFIX.length();
    public static final String ORACLE_NAME_MAPPING_TABLE = "atlasdb_table_names";
    public static final String ORACLE_NAME_MAPPING_PK_CONSTRAINT =
            PRIMARY_KEY_CONSTRAINT_PREFIX + ORACLE_NAME_MAPPING_TABLE;
    public static final String ORACLE_OVERFLOW_SEQUENCE = "overflow_seq";
    public static final int ORACLE_OVERFLOW_THRESHOLD = 2000;

    public static final String NAMESPACE_PREFIX = "_n_";
    public static final String NAMESPACE_SHORT_COLUMN_NAME = "s";
    public static final byte[] NAMESPACE_SHORT_COLUMN_BYTES = PtBytes.toBytes(NAMESPACE_SHORT_COLUMN_NAME);

    public static final TableReference PARTITION_MAP_TABLE = TableReference.createWithEmptyNamespace("_partition_map");
    public static final byte[] EMPTY_TABLE_METADATA = {}; // use carefully
    public static final byte[] GENERIC_TABLE_METADATA =
            TableMetadata.builder().nameLogSafety(LogSafety.SAFE).build().persistToBytes();

    public static final int MINIMUM_COMPRESSION_BLOCK_SIZE_KB = 4;
    public static final int DEFAULT_INDEX_COMPRESSION_BLOCK_SIZE_KB = 4;
    public static final int DEFAULT_TABLE_COMPRESSION_BLOCK_SIZE_KB = 8;
    public static final int DEFAULT_TABLE_WITH_RANGESCANS_COMPRESSION_BLOCK_SIZE_KB = 64;

    public static final int DEFAULT_TABLES_TO_PUBLISH_TABLE_LEVEL_METRICS = 10;

    public static final long STARTING_TS = 1L;
    public static final long TRANSACTION_TS = 0L;
    public static final long MAX_TS = Long.MAX_VALUE;

    public static final byte[] DEFAULT_METADATA_COORDINATION_KEY = PtBytes.toBytes("m");

    public static final long DEFAULT_TRANSACTION_LOCK_ACQUIRE_TIMEOUT_MS = 60_000;
    public static final int THRESHOLD_FOR_LOGGING_LARGE_NUMBER_OF_TRANSACTION_LOOKUPS = 10_000_000;

    public static final ImmutableSet<TableReference> HIDDEN_TABLES = ImmutableSet.of(
            TransactionConstants.TRANSACTION_TABLE,
            TransactionConstants.TRANSACTIONS2_TABLE,
            PUNCH_TABLE,
            OLD_SCRUB_TABLE,
            SCRUB_TABLE,
            NAMESPACE_TABLE,
            PARTITION_MAP_TABLE,
            PERSISTED_LOCKS_TABLE,
            SWEEP_PROGRESS_TABLE,
            COORDINATION_TABLE,
            DEFAULT_SCHEMA_METADATA_TABLE,
            SWEEP_PROGRESS_V2,
            SWEEP_PROGRESS_V1_5);

    /**
     * Tables that must always be on a KVS that supports an atomic putUnlessExists operation.
     */
    public static final ImmutableSet<TableReference> ATOMIC_TABLES = ImmutableSet.of(
            TransactionConstants.TRANSACTION_TABLE,
            TransactionConstants.TRANSACTIONS2_TABLE,
            NAMESPACE_TABLE,
            PERSISTED_LOCKS_TABLE,
            COORDINATION_TABLE);

    /**
     * Some key-value services may support atomic put unless exists and check and touch operations, but fail to
     * guarantee repeatable reads in the general case. They may offer (potentially more costly) options for reading
     * cells that do support repeatable reads.
     *
     * Where applicable, tables in this set should ideally not be read frequently. Implementers are encouraged to
     * provide alternative solutions in cases where the tables are read frequently and/or read performance is
     * critical. See ResilientCommitTimestampPutUnlessExistsTable for an example of how to work around such
     * limitations for the {@link TransactionConstants#TRANSACTIONS2_TABLE}.
     */
    public static final ImmutableSet<TableReference> SERIAL_CONSISTENCY_ATOMIC_TABLES =
            ImmutableSet.of(COORDINATION_TABLE);

    /**
     * These tables are atomic tables, but are not intended to be read in a high-cost mode. The intention of this set
     * is to ensure that decisions are made the set of {@link #ATOMIC_TABLES}.
     */
    public static final ImmutableSet<TableReference> NON_SERIAL_CONSISTENCY_ATOMIC_TABLES = ImmutableSet.of(
            TransactionConstants.TRANSACTION_TABLE, // Bankruptcy
            TransactionConstants.TRANSACTIONS2_TABLE, // Bankruptcy for Transactions2, handled for Transactions3
            NAMESPACE_TABLE, // Used for KvTableMappingService, only by Oracle
            PERSISTED_LOCKS_TABLE // Maintained for legacy purposes
            );

    public static final ImmutableSet<TableReference> TABLES_KNOWN_TO_BE_POORLY_DESIGNED =
            ImmutableSet.of(TableReference.createWithEmptyNamespace("resync_object"));

    public static final long DEFAULT_TRANSACTION_READ_TIMEOUT = 60 * 60 * 1000; // one hour
    public static final long DEFAULT_PUNCH_INTERVAL_MILLIS = 60 * 1000; // one minute

    public static final boolean DEFAULT_BACKGROUND_SCRUB_AGGRESSIVELY = true;
    public static final int DEFAULT_BACKGROUND_SCRUB_THREADS = 8;
    public static final int DEFAULT_BACKGROUND_SCRUB_READ_THREADS = 8;
    public static final long DEFAULT_BACKGROUND_SCRUB_FREQUENCY_MILLIS = 5 * 60 * 1000; // 5 minutes
    public static final int DEFAULT_BACKGROUND_SCRUB_BATCH_SIZE = 2000;
    public static final long SCRUBBER_RETRY_DELAY_MILLIS = 500L;
    public static final char OLD_SCRUB_TABLE_SEPARATOR_CHAR = '\0';

    public static final boolean DEFAULT_INITIALIZE_ASYNC = AtlasDbFactory.DEFAULT_INITIALIZE_ASYNC;

    public static final long DEFAULT_SWEEP_PAUSE_MILLIS = 5 * 1000;
    public static final long DEFAULT_SWEEP_PERSISTENT_LOCK_WAIT_MILLIS = 30_000L;
    public static final int DEFAULT_SWEEP_DELETE_BATCH_HINT = 128;
    public static final int DEFAULT_SWEEP_CANDIDATE_BATCH_HINT = 128;
    public static final int DEFAULT_SWEEP_READ_LIMIT = 128;
    public static final int DEFAULT_SWEEP_CASSANDRA_READ_THREADS = 16;
    public static final int DEFAULT_SWEEP_WRITE_THRESHOLD = 1 << 12;
    public static final long DEFAULT_SWEEP_WRITE_SIZE_THRESHOLD = 1 << 25;

    public static final boolean DEFAULT_ENABLE_SWEEP_QUEUE_WRITES = true;
    public static final boolean DEFAULT_ENABLE_TARGETED_SWEEP = true;
    public static final int LEGACY_DEFAULT_TARGETED_SWEEP_SHARDS = 1;
    public static final int DEFAULT_TARGETED_SWEEP_SHARDS = 16;
    public static final int DEFAULT_TARGETED_SWEEP_THREADS = 1;
    public static final int MAX_SWEEP_QUEUE_SHARDS = TargetedSweepMetadata.MAX_SHARDS;

    public static final int DEFAULT_STREAM_IN_MEMORY_THRESHOLD = 4 * 1024 * 1024;

    public static final long DEFAULT_TIMESTAMP_CACHE_SIZE = 1_000_000;

    public static final int MAX_TABLE_PREFIX_LENGTH = 7;
    public static final int MAX_OVERFLOW_TABLE_PREFIX_LENGTH = 6;

    public static final int DEFAULT_LOCK_TIMEOUT_SECONDS = 120;

    public static final int CASSANDRA_TABLE_NAME_CHAR_LIMIT = 48;
    public static final int POSTGRES_TABLE_NAME_CHAR_LIMIT = 63;

    public static final int TRANSACTION_TIMESTAMP_LOAD_BATCH_LIMIT = 50_000;

    public static final String SCHEMA_V2_TABLE_NAME = "V2Table";
}
