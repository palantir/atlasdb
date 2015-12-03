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
package com.palantir.atlasdb;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;

public class AtlasDbConstants {
    public static final Logger PERF_LOG = LoggerFactory.getLogger("dualschema.perf");

    public static final String TEMP_TABLE_PREFIX = "_t";

    public static final String PUNCH_TABLE = "_punch";
    public static final String SCRUB_TABLE = "_scrub";
    public static final String NAMESPACE_TABLE = "_namespace";
    public static final String NAMESPACE_PREFIX = "_n_";
    public static final String NAMESPACE_SHORT_COLUMN_NAME = "s";
    public static final byte[] NAMESPACE_SHORT_COLUMN_BYTES = PtBytes.toBytes(NAMESPACE_SHORT_COLUMN_NAME);

    public static final String PARTITION_MAP_TABLE = "_partition_map";
    public static final char SCRUB_TABLE_SEPARATOR_CHAR = '\0';
    public static final byte[] EMPTY_TABLE_METADATA = {}; // use carefully
    public static final byte[] GENERIC_TABLE_METADATA = new TableMetadata().persistToBytes();

    public static final long SCRUBBER_RETRY_DELAY_MILLIS = 2000L;

    public static final int MINIMUM_COMPRESSION_BLOCK_SIZE_KB = 4;
    public static final int DEFAULT_INDEX_COMPRESSION_BLOCK_SIZE_KB = 4;
    public static final int DEFAULT_TABLE_COMPRESSION_BLOCK_SIZE_KB = 8;
    public static final int DEFAULT_TABLE_WITH_RANGESCANS_COMPRESSION_BLOCK_SIZE_KB = 64;

    public static final long TRANSACTION_TS = 0L;

    // TODO (ejin): Organize constants (maybe into a single class?)
    public static final Set<String> hiddenTables = ImmutableSet.of(
            TransactionConstants.TRANSACTION_TABLE,
            PUNCH_TABLE,
            SCRUB_TABLE,
            NAMESPACE_TABLE,
            PARTITION_MAP_TABLE);
    public static final Set<String> SKIP_POSTFILTER_TABLES = ImmutableSet.of(TransactionConstants.TRANSACTION_TABLE,
            NAMESPACE_TABLE);

    /**
     * Tables that must always be on a KVS that supports an atomic putUnlessExists operation.
     */
    public static final Set<String> ATOMIC_TABLES = ImmutableSet.of(
            TransactionConstants.TRANSACTION_TABLE,
            NAMESPACE_TABLE);

    public static final Set<String> TABLES_KNOWN_TO_BE_POORLY_DESIGNED = ImmutableSet.of("resync_object");

    public static final long DEFAULT_TRANSACTION_READ_TIMEOUT = 60 * 60 * 1000; // one hour
    public static final long DEFAULT_PUNCH_INTERVAL_MILLIS = 60 * 1000; // one minute
    public static final boolean DEFAULT_BACKGROUND_SCRUB_AGGRESSIVELY = false;
    public static final int DEFAULT_BACKGROUND_SCRUB_THREADS = 8;
    public static final long DEFAULT_BACKGROUND_SCRUB_FREQUENCY_MILLIS = 3600000L;
    public static final int DEFAULT_BACKGROUND_SCRUB_BATCH_SIZE = 2000;
    public static final boolean DEFAULT_ENABLE_SWEEP = false;
    public static final long DEFAULT_SWEEP_PAUSE_MILLIS = 5 * 1000;
    public static final int DEFAULT_SWEEP_BATCH_SIZE = 1000;
}
