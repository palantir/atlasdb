package com.palantir.atlasdb.keyvalue.partition.exception;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.keyvalue.partition.api.PartitionMap;
import com.palantir.atlasdb.keyvalue.partition.util.VersionedObject;
import com.palantir.atlasdb.keyvalue.remoting.RemotingPartitionMapService;
import com.palantir.common.annotation.Immutable;


@Immutable public class VersionTooOldException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String pmsUri;

    public VersionedObject<PartitionMap> getUpdatedMap() {
    	return RemotingPartitionMapService.createClientSide(
    			Preconditions.checkNotNull(pmsUri)).get();
    }

    public VersionTooOldException(String pmsUri) {
    	this.pmsUri = pmsUri;
    }
    
    public VersionTooOldException() {
    	this.pmsUri = null;
    }

	@Override
	public String toString() {
		return "VersionTooOldException [pmsUri=" + pmsUri + "]";
	}
}
