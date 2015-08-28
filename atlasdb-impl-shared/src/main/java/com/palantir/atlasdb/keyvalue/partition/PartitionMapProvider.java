package com.palantir.atlasdb.keyvalue.partition;

import com.google.common.base.Function;
import com.palantir.atlasdb.keyvalue.partition.api.DynamicPartitionMap;
import com.palantir.atlasdb.keyvalue.partition.exception.VersionTooOldException;

/**
 * This is to make sure that no one extending this class
 * can access the partitionMap directly.
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
    		// Let the higher layer retry the operation with the updated map.
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
