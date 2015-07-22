// Copyright 2015 Palantir Technologies
//
// Licensed under the BSD-3 License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://opensource.org/licenses/BSD-3-Clause
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.common.supplier;

import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.Maps;
import com.palantir.common.concurrent.ExecutorInheritableThreadLocal;

/**
 * This class it used to send {@link ServiceContext}s to remote services.  Any contexts with non-null
 * state will be passed over the wire to the next spring remote call.
 * <p>
 * On the remote side any passed contexts will be available in {@link RemoteAttributeInbox}
 * <p>
 * The common way to use this class is with {@link PopulateServiceContextProxy}.
 * <p>
 * <pre>
 * ProxyClass remoteProxy = ...;
 * UserToken passThisOverTheWire = ...;
 * ServiceContext<UserToken> context = RemoteContextHolder.OUTBOX.getProviderForKey(USER_TOKEN_CONTEXT_ENUM);
 * remoteProxy = PopulateServiceContextProxy.newProxyInstanceWithConstantValue(ProxyClass.class, remoteProxy, passThisOverTheWire, context);
 * </pre>
 * <p>
 * <pre>
 * Supplier<UserToken> passToLongLivedService = RemoteContextHolder.INBOX.getProviderForKey(USER_TOKEN_CONTEXT_ENUM);
 * </pre>
 * @author carrino
 * @see PopulateServiceContextProxy
 */
public class RemoteContextHolder {
    public static final RemoteContextHolder OUTBOX = new RemoteContextHolder() {
        @Override
        public ServiceContext<Map<RemoteContextType<?>, Object>> getHolderContext() {
            return ServiceContexts.fromSupplier(super.getHolderContext());
        }
    };
    public static final RemoteContextHolder INBOX = new RemoteContextHolder() {
        @Override
        public <T, S extends java.lang.Enum<S> & RemoteContextType<T>> ServiceContext<T> getProviderForKey(S key) {
            return ServiceContexts.fromSupplier(super.getProviderForKey(key));
        }
    };

    private RemoteContextHolder() {/**/}

    private ExecutorInheritableThreadLocal<Map<RemoteContextType<?>, Object>> holder = new ExecutorInheritableThreadLocal<Map<RemoteContextType<?>, Object>>() {
        @Override
        protected Map<RemoteContextType<?>, Object> initialValue() {
            return Maps.newHashMap();
        }

        @Override
        protected Map<RemoteContextType<?>, Object> childValue(Map<RemoteContextType<?>, Object> parentValue) {
            return Maps.newHashMap(parentValue);
        }
    };

    public static <T> T newProxyCopyingOutboxToInbox(Class<T> interfaceClass, T delegate) {
        ServiceContext<Map<RemoteContextType<?>, Object>> outboxContext = OUTBOX.getHolderContextInternal();
        ServiceContext<Map<RemoteContextType<?>, Object>> inboxContext = INBOX.getHolderContext();
        T setOutboxToNull = PopulateServiceContextProxy.newProxyInstanceWithConstantValue(
                interfaceClass,
                delegate,
                null,
                outboxContext);
        return PopulateServiceContextProxy.newProxyInstance(interfaceClass, setOutboxToNull, outboxContext, inboxContext);
    }

    /**
     * This interface is used in the key to pass values over the wire to remote services.
     * This type exists for serialization and type safety.
     * <p>
     * {@link RemoteContextHolder#getProviderForKey(Enum)} takes an enum that implements this interface.
     * @see RemoteContextHolder
     * @see RemoteAttributeInbox
     */
    public interface RemoteContextType<T> {
        Class<T> getValueType();
    }

    private ServiceContext<Map<RemoteContextType<?>, Object>> getHolderContextInternal() {
        return new AbstractWritableServiceContext<Map<RemoteContextType<?>, Object>>() {
            @Override
            public Map<RemoteContextType<?>, Object> get() {
                Map<RemoteContextType<?>, Object> ret = holder.get();
                if (ret.isEmpty()) {
                    holder.remove();
                }
                return Maps.newHashMap(ret);
            }

            @Override
            public void set(Map<RemoteContextType<?>, Object> value) {
                if (value == null || value.isEmpty()) {
                    holder.remove();
                } else {
                    holder.set(Maps.newHashMap(value));
                }
            }
        };
    }

    public ServiceContext<Map<RemoteContextType<?>, Object>> getHolderContext() {
        return getHolderContextInternal();
    }

    public <T, S extends Enum<S> & RemoteContextType<T>> ServiceContext<T> getProviderForKey(@Nonnull final S key) {
        if (key == null) {
            throw new IllegalArgumentException("key cannot be null");
        }
        return new AbstractWritableServiceContext<T>() {
            @Override
            protected void set(T value) {
                if (value == null) {
                    holder.get().remove(key);
                    if (holder.get().isEmpty()) {
                        holder.remove();
                    }
                } else {
                    holder.get().put(key, value);
                }
            }

            @Override
            public T get() {
                Map<RemoteContextType<?>, Object> map = holder.get();
                if (map.isEmpty()) {
                    holder.remove();
                    return null;
                }
                return key.getValueType().cast(map.get(key));
            }
        };
    }
}
