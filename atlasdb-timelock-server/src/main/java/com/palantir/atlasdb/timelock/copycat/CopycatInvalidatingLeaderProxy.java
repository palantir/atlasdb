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
package com.palantir.atlasdb.timelock.copycat;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import javax.ws.rs.ServiceUnavailableException;

import org.immutables.value.Value;

import com.google.common.reflect.AbstractInvocationHandler;
import com.palantir.atlasdb.timelock.atomix.ImmutableLeaderAndTerm;
import com.palantir.atlasdb.timelock.atomix.LeaderAndTerm;

import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.server.CopycatServer;

public class CopycatInvalidatingLeaderProxy<T> extends AbstractInvocationHandler {
    private final CopycatServer copycatServer;
    private final CopycatClient copycatClient;
    private final AtomicReference<TermWrapped<T>> delegateRef;
    private final Supplier<T> delegateSupplier;

    private CopycatInvalidatingLeaderProxy(
            CopycatServer copycatServer,
            CopycatClient copycatClient,
            Supplier<T> delegateSupplier) {
        this.copycatServer = copycatServer;
        this.copycatClient = copycatClient;
        this.delegateRef = new AtomicReference<>();
        this.delegateSupplier = delegateSupplier;
    }

    public static <T> T create(
            CopycatServer server,
            CopycatClient client,
            Supplier<T> delegateSupplier,
            Class<T> interfaceClass) {
        CopycatInvalidatingLeaderProxy<T> proxy = new CopycatInvalidatingLeaderProxy<>(
                server,
                client,
                delegateSupplier);

        return (T) Proxy.newProxyInstance(
                interfaceClass.getClassLoader(),
                new Class<?>[] { interfaceClass, Closeable.class },
                proxy);
    }

    @Override
    protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
        LeaderAndTerm currentLeaderInfo = getLeaderInfo();
        if (currentLeaderInfo != null && isLeader(Integer.valueOf(currentLeaderInfo.leaderId()))) {
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
                String.format("This node (%s) is not the leader (%s)",
                        copycatServer.cluster().member().id(),
                        currentLeaderInfo),
                0L);
    }

//    private LeaderAndTerm getLeaderInfo() {
//        try {
//            return copycatClient.submit(ImmutableGetLeaderQuery.builder().build()).join();
//        } catch (CompletionException e) {
//            throw new ServiceUnavailableException("Couldn't seem to contact the cluster. We may be in a partition.");
//        }
//    }

    private LeaderAndTerm getLeaderInfo() {
        // This is safe for Timestamp. NOT FOR LOCKS!
        if (copycatServer.cluster().leader() == null) {
            return null;
        }
        return ImmutableLeaderAndTerm.of(
                copycatServer.cluster().term(),
                String.valueOf(copycatServer.cluster().leader().id()));
    }

    private boolean isLeader(int leader) {
        return Objects.equals(copycatServer.cluster().member().id(), leader);
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
