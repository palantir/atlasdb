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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import javax.ws.rs.ServiceUnavailableException;

import org.immutables.value.Value;

import com.google.common.base.Throwables;
import com.google.common.reflect.AbstractInvocationHandler;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.UncheckedExecutionException;

import io.atomix.group.LocalMember;
import io.atomix.group.election.Election;
import io.atomix.variables.DistributedValue;

public final class InvalidatingLeaderProxy<T> extends AbstractInvocationHandler {
    private final LocalMember localMember;
    private final DistributedValue<String> leaderId;
    private final AtomicReference<TermAndDelegate> delegateRef = new AtomicReference<>();
    private final Supplier<T> delegateSupplier;

    private InvalidatingLeaderProxy(
            LocalMember localMember,
            DistributedValue<String> leaderId,
            Supplier<T> delegateSupplier) {
        this.localMember = localMember;
        this.leaderId = leaderId;
        this.delegateSupplier = delegateSupplier;
    }

    public static <T> T create(
            LocalMember localMember,
            DistributedValue<String> leaderId,
            Election election,
            Supplier<T> delegateSupplier,
            Class<T> interfaceClass) {
        InvalidatingLeaderProxy<T> proxy = new InvalidatingLeaderProxy<>(
                localMember,
                leaderId,
                delegateSupplier);

        election.onElection(term -> proxy.clearDelegateUnchecked());

        return (T) Proxy.newProxyInstance(
                interfaceClass.getClassLoader(),
                new Class<?>[] { interfaceClass, Closeable.class },
                proxy);
    }

    @Override
    protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
        LeaderAndTerm currentLeaderId = getLeaderId();
        if (currentLeaderId != null && isLeader(currentLeaderId.getLeader())) {
            TermAndDelegate delegate = delegateRef.get();
            while (delegate == null) {
                TermAndDelegate newDelegate = ImmutableTermAndDelegate.of(currentLeaderId.getTerm(), delegateSupplier.get());
                delegateRef.compareAndSet(null, newDelegate);
                delegate = delegateRef.get();
            }

            if (delegate.getTerm() == currentLeaderId.getTerm()) {
                return method.invoke(delegate.getDelegate(), args);
            }
        }
        clearDelegate();
        throw new ServiceUnavailableException(
                String.format("This node (%s) is not the leader (%s)", getLocalId(), currentLeaderId),
                0L);
    }

    private boolean isLeader(String leader) {
        return Objects.equals(getLocalId(), leader);
    }

    private String getLocalId() {
        return localMember.id();
    }

    private LeaderAndTerm getLeaderId() {
        try {
            return LeaderAndTerm.fromStoredString(Futures.getUnchecked(leaderId.get()));
        } catch (UncheckedExecutionException e) {
            if (e.getCause() instanceof IOException) {
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

    private void clearDelegateUnchecked() {
        try {
            clearDelegate();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private void clearDelegate() throws IOException {
        TermAndDelegate delegate = delegateRef.getAndSet(null);
        if (delegate != null && delegate.getDelegate() instanceof Closeable) {
            ((Closeable) delegate.getDelegate()).close();
        }
    }

    @Value.Immutable
    interface TermAndDelegate {
        @Value.Parameter
        long getTerm();

        @Value.Parameter
        Object getDelegate();
    }
}
