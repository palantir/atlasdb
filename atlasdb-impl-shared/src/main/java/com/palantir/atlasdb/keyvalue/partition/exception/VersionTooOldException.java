package com.palantir.atlasdb.keyvalue.partition.exception;

import com.palantir.atlasdb.keyvalue.partition.api.PartitionMap;
import com.palantir.atlasdb.keyvalue.partition.util.VersionedObject;
import com.palantir.atlasdb.keyvalue.remoting.RemotingPartitionMapService;

public class VersionTooOldException extends RuntimeException {

    private static final long serialVersionUID = 1L;
    private final long minRequiredVersion;
    private final String pmsUri;

    public VersionTooOldException(long minRequiredVersion, String pmsUri) {
        this(minRequiredVersion, pmsUri, null);
    }

    public VersionTooOldException(long minRequiredVersion, String pmsUri, String message) {
        super(message);
        this.pmsUri = pmsUri;
        this.minRequiredVersion = minRequiredVersion;
    }

    public VersionTooOldException() {
        this.pmsUri = null;
        this.minRequiredVersion = -1L;
    }

    public long getMinRequiredVersion() {
        return minRequiredVersion;
    }

    public VersionedObject<PartitionMap> getNewVersion() {
        return RemotingPartitionMapService.createClientSide(pmsUri).get();
    }

}
