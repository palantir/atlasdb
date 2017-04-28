/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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
package org.apache.cassandra.db.compaction;

import java.util.List;
import java.util.Map;

import javax.management.openmbean.TabularData;

public interface CompactionManagerMBean {
    /**
     * List of running compaction objects.
     */
    List<Map<String, String>> getCompactions();

    /**
     * List of running compaction summary strings.
     */
    List<String> getCompactionSummary();

    /**
     * Get the compaction history.
     */
    TabularData getCompactionHistory();

    /**
     * Triggers the compaction of user specified sstables.
     * You can specify files from various keyspaces and columnfamilies.
     * If you do so, user defined compaction is performed several times to the groups of files
     * in the same keyspace/columnfamily.
     *
     * @param dataFiles a comma separated list of sstable file to compact.
     *                  must contain keyspace and columnfamily name in path(for 2.1+) or file name itself.
     */
    void forceUserDefinedCompaction(String dataFiles);

    /**
     * Stop all running compaction-like tasks having the provided {@code type}.
     * @param type the type of compaction to stop. Can be one of:
     *   - COMPACTION
     *   - VALIDATION
     *   - CLEANUP
     *   - SCRUB
     *   - INDEX_BUILD
     */
    void stopCompaction(String type);

    /**
     * Stop an individual running compaction using the compactionId.
     *
     * @param compactionId Compaction ID of compaction to stop. Such IDs can be found in
     *                     the compactions_in_progress table of the system keyspace.
     */
    void stopCompactionById(String compactionId);

    /**
     * Returns core size of compaction thread pool.
     */
    int getCoreCompactorThreads();

    /**
     * Allows user to resize maximum size of the compaction thread pool.
     * @param number New maximum of compaction threads
     */
    void setCoreCompactorThreads(int number);

    /**
     * Returns maximum size of compaction thread pool.
     */
    int getMaximumCompactorThreads();

    /**
     * Allows user to resize maximum size of the compaction thread pool.
     *
     * @param number New maximum of compaction threads
     */
    void setMaximumCompactorThreads(int number);

    /**
     * Returns core size of validation thread pool.
     */
    int getCoreValidationThreads();

    /**
     * Allows user to resize maximum size of the compaction thread pool.
     *
     * @param number New maximum of compaction threads
     */
    void setCoreValidationThreads(int number);

    /**
     * Returns size of validator thread pool.
     */
    int getMaximumValidatorThreads();

    /**
     * Allows user to resize maximum size of the validator thread pool.
     * @param number New maximum of validator threads
     */
    void setMaximumValidatorThreads(int number);
}
