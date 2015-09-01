package com.palantir.atlasdb.keyvalue.partition;

import com.google.common.base.Function;
import com.palantir.atlasdb.keyvalue.partition.api.DynamicPartitionMap;
import com.palantir.atlasdb.keyvalue.partition.exception.VersionTooOldException;

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
    	} catch (VersionTooOldException e) {
    		partitionMap = e.getUpdatedMap();
    		/**
    		 * Update the map but let the transaction manager retry the task.
    		 * It seems to be reasonable since some of the KVS operations
    		 * are not idempotent so retrying them from here could get
    		 * other errors that would confuse the transaction manager.
    		 */
    		throw e;
    	}
    }

    protected void updatePartitionMap(DynamicPartitionMap partitionMap) {
    	this.partitionMap = partitionMap;
    }

    protected PartitionMapProvider(DynamicPartitionMap partitionMap) {
    	this.partitionMap = partitionMap;
    }
}
