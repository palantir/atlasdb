/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.timelock.atomix;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import javax.ws.rs.ServiceUnavailableException;

import org.immutables.value.Value;

import com.google.common.reflect.AbstractInvocationHandler;
import com.google.common.util.concurrent.UncheckedExecutionException;

import io.atomix.group.LocalMember;
import io.atomix.variables.DistributedValue;

public final class InvalidatingLeaderProxy<T> extends AbstractInvocationHandler {
    private final LocalMember localMember;
    private final DistributedValue<LeaderAndTerm> leaderInfo;
    private final AtomicReference<TermWrapped<T>> delegateRef = new AtomicReference<>();
    private final Supplier<T> delegateSupplier;

    private InvalidatingLeaderProxy(
            LocalMember localMember,
            DistributedValue<LeaderAndTerm> leaderInfo,
            Supplier<T> delegateSupplier) {
        this.localMember = localMember;
        this.leaderInfo = leaderInfo;
        this.delegateSupplier = delegateSupplier;
    }

    public static <T> T create(
            LocalMember localMember,
            DistributedValue<LeaderAndTerm> leaderInfo,
            Supplier<T> delegateSupplier,
            Class<T> interfaceClass) {
        InvalidatingLeaderProxy<T> proxy = new InvalidatingLeaderProxy<>(
                localMember,
                leaderInfo,
                delegateSupplier);

        return (T) Proxy.newProxyInstance(
                interfaceClass.getClassLoader(),
                new Class<?>[] { interfaceClass, Closeable.class },
                proxy);
    }

    @Override
    protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
        LeaderAndTerm currentLeaderInfo = getLeaderInfo();
        if (currentLeaderInfo != null && isLeader(currentLeaderInfo.leaderId())) {
            TermWrapped<T> delegate = delegateRef.get();
            while (delegate == null || delegate.term() < currentLeaderInfo.term()) {
                TermWrapped<T> newDelegate = ImmutableTermWrapped.of(currentLeaderInfo.term(), delegateSupplier.get());
                if (delegateRef.compareAndSet(delegate, newDelegate)) {
                    closeIfNecessary(delegate);
                } else {
                    closeIfNecessary(newDelegate);
                }
                delegate = delegateRef.get();
            }

            if (delegate.term() == currentLeaderInfo.term()) {
                return method.invoke(delegate.delegate(), args);
            }
        }
        clearDelegate();
        throw new ServiceUnavailableException(
                String.format("This node (%s) is not the leader (%s)", getLocalId(), currentLeaderInfo),
                0L);
    }

    private boolean isLeader(String leaderId) {
        return Objects.equals(getLocalId(), leaderId);
    }

    private String getLocalId() {
        return localMember.id();
    }

    private LeaderAndTerm getLeaderInfo() {
        try {
            return AtomixRetryer.getWithRetry(leaderInfo::get);
        } catch (UncheckedExecutionException e) {
            if (rootCauseIsIoException(e)) {
                throw new ServiceUnavailableException(
                        String.format(
                                "Could not contact the cluster. Has this node (%s) been partitioned off?",
                                getLocalId()),
                        0L,
                        e);
            }
            throw e;
        }
    }

    private boolean rootCauseIsIoException(UncheckedExecutionException ex) {
        return ex.getCause() instanceof CompletionException
                && ex.getCause().getCause() instanceof IOException;
    }

    private void clearDelegate() throws IOException {
        TermWrapped<T> wrappedDelegate = delegateRef.getAndSet(null);
        closeIfNecessary(wrappedDelegate);
    }

    private void closeIfNecessary(TermWrapped<T> wrappedDelegate) throws IOException {
        if (wrappedDelegate != null && wrappedDelegate.delegate() instanceof Closeable) {
            ((Closeable) wrappedDelegate.delegate()).close();
        }
    }

    @Value.Immutable
    interface TermWrapped<U> {
        @Value.Parameter
        long term();

        @Value.Parameter
        U delegate();
    }
}
