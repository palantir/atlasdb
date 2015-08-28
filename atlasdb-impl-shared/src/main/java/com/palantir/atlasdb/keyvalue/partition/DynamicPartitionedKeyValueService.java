package com.palantir.atlasdb.keyvalue.partition;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.hibernate.validator.internal.util.privilegedactions.NewInstance;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.partition.api.PartitionMap;
import com.palantir.atlasdb.keyvalue.partition.exception.VersionTooOldException;
import com.palantir.common.concurrent.PTExecutors;

public class DynamicPartitionedKeyValueService extends PartitionedKeyValueService {

    PartitionMap partitionMap;

    private DynamicPartitionedKeyValueService(PartitionMap partitionMap) {
        super(PTExecutors.newCachedThreadPool(), new QuorumParameters(3, 3, 3));
        this.partitionMap = partitionMap;
    }
    
    public static KeyValueService create(PartitionMap partitionMap) {
    	DynamicPartitionedKeyValueService pkvs = new DynamicPartitionedKeyValueService(partitionMap);
    	return newInstance(pkvs);
    }
    
    @Override
    protected PartitionMap getPartitionMap() {
        return partitionMap;
    }

    public static KeyValueService newInstance(DynamicPartitionedKeyValueService delegate) {
        AutoUpdatePartitionMapProxy handler = new AutoUpdatePartitionMapProxy(delegate);
        return (KeyValueService) Proxy.newProxyInstance(KeyValueService.class.getClassLoader(), new Class<?>[] { KeyValueService.class }, handler);
    }

    public static class AutoUpdatePartitionMapProxy implements InvocationHandler {
        final DynamicPartitionedKeyValueService delegate;

        private AutoUpdatePartitionMapProxy(DynamicPartitionedKeyValueService delegate) {
            this.delegate = delegate;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            try {
                return method.invoke(delegate, args);
            } catch (InvocationTargetException e) {
                Throwable cause = e.getCause();
                if (cause instanceof VersionTooOldException) {
                    VersionTooOldException vtoe = (VersionTooOldException) cause;
                    delegate.partitionMap = vtoe.getUpdatedMap().getObject();
                    return invoke(proxy, method, args);
                }
                throw cause;
            }
        }
    }

	@Override
	protected void updatePartitionMap(PartitionMap partitionMap) {
		this.partitionMap = partitionMap;
	}

}
