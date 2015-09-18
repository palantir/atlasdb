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
package com.palantir.atlasdb.keyvalue.partition;

import javax.management.RuntimeErrorException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.partition.api.DynamicPartitionMap;
import com.palantir.atlasdb.keyvalue.partition.exception.ClientVersionTooOldException;
import com.palantir.atlasdb.keyvalue.partition.exception.EndpointVersionTooOldException;
import com.palantir.atlasdb.keyvalue.partition.map.InMemoryPartitionMapService;
import com.palantir.atlasdb.keyvalue.partition.map.PartitionMapService;

/**
 * This is to make sure that no one extending this class
 * can access the partitionMap directly.
 *
 * Still care needs to be taken not to leak any direct
 * or indirect DynamicPartitionMap references to outside of
 * the runWithPartitionMap method.
 *
 * @author htarasiuk
 *
 */
public class PartitionMapProvider {

    private static final Logger log = LoggerFactory.getLogger(PartitionMapProvider.class);

    private final ImmutableList<PartitionMapService> partitionMapProviders;
    private final int partitionMapProvidersReadFactor;
    private final PartitionMapService localService = InMemoryPartitionMapService.createEmpty();

    protected <T> T runWithPartitionMapRetryable(Function<? super DynamicPartitionMap, T> task) {
        while (true) {
            try {
                return runWithPartitionMap(task);
            } catch (ClientVersionTooOldException | EndpointVersionTooOldException e) {
                // New version should be used now, retry.
                e.printStackTrace(System.out);
                log.info("Retrying...");
            }
        }
    }

    protected <T> T runWithPartitionMap(Function<? super DynamicPartitionMap, T> task) {
        try {
            log.info("Running task with pm version=" + localService.getMapVersion());
            return task.apply(localService.getMap());
        } catch (ClientVersionTooOldException e) {
            log.info("Downloading partition map from endpoint");
            localService.updateMapIfNewer(e.getUpdatedMap());
            log.info("Downloaded version " + localService.getMapVersion());
            /**
             * Update the map but let the transaction manager retry the task.
             * It seems to be reasonable since some of the KVS operations
             * are not idempotent so retrying them from here could get
             * other errors that would confuse the transaction manager.
             */
            throw e;
        } catch (EndpointVersionTooOldException e) {
            log.info("Pushing local partition map to endpoint");
            e.pushNewMap(localService.getMap());
            /**
             * Push my map version to the endpoint but let the transaction
             * manager retry this task for same reasons as above.
             */
            throw e;
        } catch (RuntimeException e) {
            /**
             * Consult the seed partition map servers to ensure that my map is
             * up-to-date.
             *
             * TODO: Use quorum instead of first success?
             *
             */
            try {
                log.info("Trying to consult seed servers in case local partition map is out of date");
                log.info("Local map version before consulting: " + localService.getMapVersion());
                updatePartitionMapFromSeedServers(false);
                log.info("Local map version after consulting: " + localService.getMapVersion());
            } catch (RuntimeErrorException re) {
                log.warn("Error while trying to update map from seed servers.");
                re.printStackTrace(System.out);
            }
            throw e;
        }
    }

    protected PartitionMapProvider(ImmutableList<PartitionMapService> partitionMapProviders, int partitionMapProvidersReadFactor) {
        this.partitionMapProviders = partitionMapProviders;
        this.partitionMapProvidersReadFactor = partitionMapProvidersReadFactor;
        updatePartitionMapFromSeedServers(true);
    }

    private void updatePartitionMapFromSeedServers(boolean mustSucceed) {
        int numSucc = 0;
        RuntimeException lastSuppressedException = null;
        for (PartitionMapService pms : partitionMapProviders) {
            try {
                localService.updateMapIfNewer(pms.getMap());
                numSucc++;
            } catch (RuntimeException re) {
                log.warn("Error when connecting to seed server:");
                re.printStackTrace(System.out);
                lastSuppressedException = re;
            }
        }

        if (numSucc < partitionMapProvidersReadFactor) {
            if (!mustSucceed) {
                log.error("Could not contact enough seed servers. Ignoring...");
            } else {
                throw lastSuppressedException;
            }
        } else {
            log.info("Seed servers consulted successfully");
        }
    }
}
