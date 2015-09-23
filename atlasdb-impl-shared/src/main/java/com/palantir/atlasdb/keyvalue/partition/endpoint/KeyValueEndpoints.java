package com.palantir.atlasdb.keyvalue.partition.endpoint;

import java.util.UUID;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.keyvalue.partition.PartitionedKeyValueConstants;

public class KeyValueEndpoints {
    private KeyValueEndpoints() {
    }

    public static String makeUniqueRackIfNoneSpecified(String rack) {
        if (PartitionedKeyValueConstants.NO_RACK.equals(rack)) {
            return UUID.randomUUID().toString();
        }
        return Preconditions.checkNotNull(rack);
    }
}
