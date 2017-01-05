/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.atlasdb.timelock.jsimpledb;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import javax.ws.rs.ServiceUnavailableException;

import org.immutables.value.Value;
import org.jsimpledb.kv.RetryTransactionException;
import org.jsimpledb.kv.raft.LeaderRole;
import org.jsimpledb.kv.raft.RaftKVDatabase;
import org.jsimpledb.kv.raft.RaftKVTransaction;

import com.google.common.reflect.AbstractInvocationHandler;

public class JSimpleDbInvalidatingLeaderProxy<T> extends AbstractInvocationHandler {
    private final RaftKVDatabase raft;
    private final Supplier<T> delegateSupplier;
    private final AtomicReference<TermWrapped<T>> delegateRef;

    public JSimpleDbInvalidatingLeaderProxy(RaftKVDatabase raft, Supplier<T> delegateSupplier) {
        this.raft = raft;
        this.delegateSupplier = delegateSupplier;
        this.delegateRef = new AtomicReference<>();
    }

    public static <T> T create(
            RaftKVDatabase raft,
            Supplier<T> delegateSupplier,
            Class<T> interfaceClass) {
        JSimpleDbInvalidatingLeaderProxy<T> proxy = new JSimpleDbInvalidatingLeaderProxy<>(
                raft,
                delegateSupplier);

        return (T) Proxy.newProxyInstance(
                interfaceClass.getClassLoader(),
                new Class<?>[] { interfaceClass, Closeable.class },
                proxy);
    }

    @Override
    protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
        try {
            if (raft.getCurrentRole() instanceof LeaderRole) {
                // We *think* we are the leader. But we still need to get more info
                RaftKVTransaction tx = raft.createTransaction();
                tx.setTimeout(RaftKVDatabase.DEFAULT_HEARTBEAT_TIMEOUT * 2);
                tx.commit(); // this "touch" means we have contacted a majority of servers
                long currentTerm = tx.getCommitTerm();

                TermWrapped<T> delegate = delegateRef.get();
                while (delegate == null || delegate.term() < currentTerm) {
                    TermWrapped<T> newDelegate = ImmutableTermWrapped.of(currentTerm, delegateSupplier.get());
                    if (delegateRef.compareAndSet(delegate, newDelegate)) {
                        closeIfNecessary(delegate);
                    } else {
                        closeIfNecessary(newDelegate);
                    }
                    delegate = delegateRef.get();
                }
                if (delegate.term() == currentTerm) {
                    return method.invoke(delegate.delegate(), args);
                }
            }
        } catch (RetryTransactionException e) {
            clearDelegate();
            throw new ServiceUnavailableException(
                    String.format("This node (%s) is not the leader.", raft.getIdentity()),
                    0L);
        }
        clearDelegate();
        throw new ServiceUnavailableException(
                String.format("This node (%s) is not the leader.", raft.getIdentity()),
                0L);
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
