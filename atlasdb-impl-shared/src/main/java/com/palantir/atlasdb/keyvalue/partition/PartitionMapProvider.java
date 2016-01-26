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
 * The wrapper methods handle VersionMismatchException by updating
 * the (local or remote) map. If a task is retryable, this will automatically
 * retry the task. Otherwise the exception will be propagated to the caller.
 *
 * It will also handle the case of a RuntimeException caused by an attempt to
 * communicate with an endpoint that has been removed from map and taken down
 * (but the local outdated client does not know about it). This is made transparent
 * to the caller, he might receive ClientVersionTooOld if non-retryable or it will
 * also retry automatically otherwise.
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
                log.info("Retrying...", e);
            }
        }
    }

    protected <T> T runWithPartitionMap(Function<? super DynamicPartitionMap, T> task) {
        final long versionBeforeRequest = localService.getMapVersion();

        try {
            log.info("Running task with pm version=" + localService.getMapVersion());
            return task.apply(localService.getMap());
        } catch (ClientVersionTooOldException e) {
            log.info("Downloading partition map from endpoint");
            localService.updateMapIfNewer(e.getUpdatedMap());
            log.info("Downloaded version " + localService.getMapVersion());
            throw e;
        } catch (EndpointVersionTooOldException e) {
            log.info("Pushing local partition map to endpoint");
            e.pushNewMap(localService.getMap());
            throw e;
        } catch (RuntimeException e) {
            /**
             * Consult the seed partition map servers to ensure that my map is
             * up-to-date.
             * If it was outdated, assume it to be the cause of exception,
             * update the map and throw ClientTooOldException to the caller.
             */
            log.info("Consulting seed servers in case local partition map is out of date");

            if (updatePartitionMapFromSeedServers(false)) {
                log.info("Local map updated from seed servers.");
            } else {
                // Just a fatal RuntimeException
                log.info("Local map not updated from seed servers.");
            }

            final long versionAfterRequest = localService.getMapVersion();
            log.info("Version before request: " + versionBeforeRequest + ", after request: " + versionAfterRequest);

            if (versionAfterRequest != versionBeforeRequest) {
                /**
                 * An update has taken place. This means that the client partition map was really
                 * out-of-date and that could be the reason we received another RuntimeException (as
                 * one of the endpoints might have been removed and taken down).
                 * Thus I propagate it as a ClientVersionTooOldException to the caller.
                 */
                assert versionAfterRequest > versionBeforeRequest;

                ClientVersionTooOldException exc = new ClientVersionTooOldException() {
                    private static final long serialVersionUID = -2500173504794874136L;
                    @Override
                    public DynamicPartitionMap getUpdatedMap() {
                        return localService.getMap();
                    }
                };
                exc.initCause(e);
                throw exc;
            } else {
                // Just another RuntimeException
                throw e;
            }
        }
    }

    protected PartitionMapProvider(ImmutableList<PartitionMapService> partitionMapProviders, int partitionMapProvidersReadFactor) {
        this.partitionMapProviders = partitionMapProviders;
        this.partitionMapProvidersReadFactor = partitionMapProvidersReadFactor;
        updatePartitionMapFromSeedServers(true);
    }

    /**
     *
     * @param mustSucceed If set to <tt>true</tt>, will throw if less than quorum
     * seed servers responded.
     * @return <tt>true</tt> if an update has taken place.
     */
    private boolean updatePartitionMapFromSeedServers(boolean mustSucceed) {
        int numSucc = 0;
        boolean updateHasTakenPlace = false;

        RuntimeException lastSuppressedException = null;

        for (PartitionMapService pms : partitionMapProviders) {
            try {
                DynamicPartitionMap newMap = pms.getMap();
                long newMapVersion = newMap.getVersion();
                long originalVersion = localService.updateMapIfNewer(newMap);

                if (newMapVersion != originalVersion) {
                    assert newMapVersion > originalVersion;
                    updateHasTakenPlace = true;
                }

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

        return updateHasTakenPlace;
    }
}
