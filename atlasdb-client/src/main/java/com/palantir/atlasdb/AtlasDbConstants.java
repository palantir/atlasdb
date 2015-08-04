/**
 * // Copyright 2015 Palantir Technologies
 * //
 * // Licensed under the BSD-3 License (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * // http://opensource.org/licenses/BSD-3-Clause
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */
package com.palantir.atlasdb;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;

public class AtlasDbConstants {
    public static final Logger PERF_LOG = LoggerFactory.getLogger("dualschema.perf");

    public static final int DEFAULT_CASSANDRA_PORT = 9160;
    public static final int DEFAULT_REPLICATION_FACTOR = 3;


    public static final long BASE_REALM_ID = 1L;
    public static final int DEFAULT_RANGE_BATCH_SIZE = 100;

    public static final long NULL_HORIZON_DATA_EVENT_ID = -1L;

    public static final String RELATIONAL_TABLE_PREFIX = "pt_met_";
    public static final String TEMP_TABLE_PREFIX = "_t";
    public static final String INDEX_SUFFIX = "idx";

    public static final String PUNCH_TABLE = "_punch";
    public static final String SCRUB_TABLE = "_scrub";
    public static final String NAMESPACE_TABLE = "_namespace";
    public static final String NAMESPACE_PREFIX = "_n_";
    public static final char SCRUB_TABLE_SEPARATOR_CHAR = '\0';

    public static final int PUNCH_INTERVAL_MILLIS = 2000;
    public static final long SCRUBBER_RETRY_DELAY_MILLIS = 1000L;
    public static final int DEFAULT_SCRUBBER_BATCH_SIZE = 1000;
    public static final int DEFAULT_SCRUBBER_THREAD_COUNT = 8;
    public static final int DEFAULT_SWEEPER_BATCH_SIZE = 1000;

    // These become overridden by Dispatch system properties
    public static final long DEFAULT_TRANSACTION_READ_TIMEOUT_MILLIS = 86400000L;
    public static final long DEFAULT_BACKGROUND_SCRUB_FREQUENCY_MILLIS = 3600000L;

    // TODO (ejin): Organize constants (maybe into a single class?)
    public static final Set<String> hiddenTables = ImmutableSet.of(
            TransactionConstants.TRANSACTION_TABLE,
            PUNCH_TABLE,
            SCRUB_TABLE,
            NAMESPACE_TABLE);
    public static final Set<String> SKIP_POSTFILTER_TABLES = ImmutableSet.of(TransactionConstants.TRANSACTION_TABLE,
            NAMESPACE_TABLE);

    /**
     * Tables that must always be on a KVS that supports an atomic putUnlessExists operation.
     */
    public static final Set<String> ATOMIC_TABLES = ImmutableSet.of(
            TransactionConstants.TRANSACTION_TABLE,
            NAMESPACE_TABLE);

    public static final long FAKE_SYSTEM_ID = 1L;

    public static final String DELETE_RESYNC_CRAWL_LOGGER = "deleteResyncCrawlLogger"; //$NON-NLS-1$

    public static final String HARD_DELETE_LOGGER = "hardDeleteLogger"; //$NON-NLS-1$
    public static final String HARD_DELETE_CHRONICLE_TAG = "hard-delete"; //$NON-NLS-1$

    public static final Set<String> TABLES_KNOWN_TO_BE_POORLY_DESIGNED = ImmutableSet.of("resync_object");
}
