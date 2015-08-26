package com.palantir.atlasdb.keyvalue.partition;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.partition.api.PartitionMap;
import com.palantir.atlasdb.keyvalue.partition.util.VersionedObject;
import com.palantir.common.supplier.PopulateServiceContextProxy;
import com.palantir.common.supplier.RemoteContextHolder;
import com.palantir.common.supplier.RemoteContextHolder.RemoteContextType;
import com.palantir.common.supplier.ServiceContext;

public class VersionedKeyValueEndpoint implements KeyValueEndpoint {

    final PartitionMapService pms;
    final KeyValueService kvs;
    final static ServiceContext<Long> provider =
            RemoteContextHolder.OUTBOX.getProviderForKey(VERSIONED_PM.PM_VERSION);

    private VersionedKeyValueEndpoint(PartitionMapService pms,
                                      Supplier<Long> localVersionProvider,
                                      KeyValueService kvs) {
        this.pms = pms;
        this.kvs = kvs;
    }

    public static KeyValueEndpoint create(PartitionMapService pms,
                                                   Supplier<Long> localVersionProvider,
                                                   KeyValueService kvs) {
        VersionedKeyValueEndpoint vkve = new VersionedKeyValueEndpoint(pms, localVersionProvider, kvs);
        return PopulateServiceContextProxy.newProxyInstance(
                KeyValueEndpoint.class, vkve,
                localVersionProvider, provider);
    }

    public <T> T run(Function<KeyValueService, T> task) throws UpdatePartitionMapException {
        try {
            return task.apply(kvs);
        } catch (VersionTooOldException e) {
            VersionedObject<PartitionMap> newPartitionMap = pms.get();
            throw new UpdatePartitionMapException(newPartitionMap.getObject(), newPartitionMap.getVersion());
        }
    }

    public enum VERSIONED_PM implements RemoteContextType<Long> {
        PM_VERSION {
            @Override
            public Class<Long> getValueType() {
                return Long.class;
            }
        }
    }

    public static class UpdatePartitionMapException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        final PartitionMap newPartitionMap;
        final long newVersion;

        public PartitionMap getNewPartitionMap() {
            return newPartitionMap;
        }

        public long getNewVersion() {
            return newVersion;
        }

        public UpdatePartitionMapException(PartitionMap newPartitionMap, long newVersion) {
            this.newPartitionMap = newPartitionMap;
            this.newVersion = newVersion;
        }
    }

    public static class VersionTooOldException extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

}
