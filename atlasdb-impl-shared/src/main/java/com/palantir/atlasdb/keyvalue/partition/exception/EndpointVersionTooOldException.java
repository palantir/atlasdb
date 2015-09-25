package com.palantir.atlasdb.keyvalue.partition.exception;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.keyvalue.partition.api.DynamicPartitionMap;
import com.palantir.atlasdb.keyvalue.remoting.RemotingPartitionMapService;
import com.palantir.common.annotation.Immutable;

@Immutable public class EndpointVersionTooOldException extends VersionMismatchException {

    private static final long serialVersionUID = 6421197986192185450L;
    private final String pmsUri;

    /**
     * Only use if pmsUri has been filled in.
     *
     * @param map
     */
    public void pushNewMap(DynamicPartitionMap map) {
        RemotingPartitionMapService.createClientSide(
                Preconditions.checkNotNull(pmsUri)).updateMapIfNewer(map);
    }

    public EndpointVersionTooOldException() {
        super(null);
        this.pmsUri = null;
    }

    public EndpointVersionTooOldException(String pmsUri) {
        super(pmsUri);
        this.pmsUri = pmsUri;
    }

    public EndpointVersionTooOldException(String message, Throwable cause) {
        super(message, cause);
        this.pmsUri = message;
    }

    @Override
    public String toString() {
        return "EndpointVersionTooOldException [pmsUri=" + pmsUri + "]";
    }

}
