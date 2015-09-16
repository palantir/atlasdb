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

import com.google.common.base.Function;
import com.palantir.atlasdb.keyvalue.partition.api.DynamicPartitionMap;
import com.palantir.atlasdb.keyvalue.partition.exception.ClientVersionTooOldException;
import com.palantir.atlasdb.keyvalue.partition.exception.EndpointVersionTooOldException;

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

    private DynamicPartitionMap partitionMap;

    protected <T> T runWithPartitionMap(Function<DynamicPartitionMap, T> task) {
        try {
            return task.apply(partitionMap);
        } catch (ClientVersionTooOldException e) {
            partitionMap = e.getUpdatedMap();
            /**
             * Update the map but let the transaction manager retry the task.
             * It seems to be reasonable since some of the KVS operations
             * are not idempotent so retrying them from here could get
             * other errors that would confuse the transaction manager.
             */
            throw e;
        } catch (EndpointVersionTooOldException e) {
            e.pushNewMap(partitionMap);
            /**
             * Push my map version to the endpoint but let the transaction
             * manager retry this task for same reasons as above.
             */
            throw e;
        } catch (RuntimeException e) {
            /**
             * TODO:
             * Consult the seed partition map servers to ensure that my map is
             * up-to-date.
             *
             */
            throw e;
        }
    }

    protected PartitionMapProvider(DynamicPartitionMap partitionMap) {
        this.partitionMap = partitionMap;
    }
}
