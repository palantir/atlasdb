/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.timelock;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.timelock.api.ConjureTimelockService;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.util.TestProxiesV2.ProxyModeV2;
import com.palantir.lock.ConjureLockV1Service;
import com.palantir.lock.LockRpcClient;
import com.palantir.lock.LockService;
import com.palantir.lock.client.NamespacedConjureTimelockService;
import com.palantir.lock.client.NamespacedConjureTimelockServiceImpl;
import com.palantir.lock.client.RemoteLockServiceAdapter;
import com.palantir.lock.client.RemoteTimelockServiceAdapter;
import com.palantir.lock.v2.DefaultNamespacedTimelockRpcClient;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.NamespacedTimelockRpcClient;
import com.palantir.lock.v2.TimelockRpcClient;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.lock.watch.LockWatchCache;
import com.palantir.lock.watch.LockWatchCacheImpl;
import com.palantir.timestamp.RemoteTimestampManagementAdapter;
import com.palantir.timestamp.TimestampManagementRpcClient;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampRange;
import org.immutables.value.Value;

@Value.Immutable
public interface NamespacedClientsV2 {

    interface ProxyFactoryV2 {
        <T> T createProxy(Class<T> clazz, ProxyModeV2 proxyMode);
    }

    @Value.Parameter
    String namespace();

    @Value.Parameter
    ProxyFactoryV2 proxyFactory();

    @Value.Parameter
    ProxyModeV2 proxyMode();

    default NamespacedClientsV2 throughWireMockProxy() {
        return ImmutableNamespacedClientsV2.of(namespace(), proxyFactory(), ProxyModeV2.WIREMOCK);
    }

    static NamespacedClientsV2 from(String namespace, ProxyFactoryV2 proxyFactory) {
        return ImmutableNamespacedClientsV2.of(namespace, proxyFactory, ProxyModeV2.DIRECT);
    }

    @Value.Derived
    default TimelockService timelockService() {
        return RemoteTimelockServiceAdapter.create(
                Namespace.of(namespace()),
                namespacedTimelockRpcClient(),
                namespacedConjureTimelockService(),
                lockWatchEventCache());
    }

    @Value.Default
    default LockWatchCache lockWatchEventCache() {
        return LockWatchCacheImpl.noOp();
    }

    @Value.Derived
    default NamespacedTimelockRpcClient namespacedTimelockRpcClient() {
        return new DefaultNamespacedTimelockRpcClient(timelockRpcClient(), namespace());
    }

    @Value.Derived
    default NamespacedConjureTimelockService namespacedConjureTimelockService() {
        return new NamespacedConjureTimelockServiceImpl(conjureTimelockService(), namespace());
    }

    @Value.Derived
    default TimelockRpcClient timelockRpcClient() {
        return proxyFactory().createProxy(TimelockRpcClient.class, proxyMode());
    }

    @Value.Derived
    default ConjureTimelockService conjureTimelockService() {
        return proxyFactory().createProxy(ConjureTimelockService.class, proxyMode());
    }

    @Value.Derived
    default LockService legacyLockService() {
        return RemoteLockServiceAdapter.create(
                proxyFactory().createProxy(LockRpcClient.class, proxyMode()), namespace());
    }

    @Value.Derived
    default ConjureLockV1Service conjureLegacyLockService() {
        return proxyFactory().createProxy(ConjureLockV1Service.class, proxyMode());
    }

    @Value.Derived
    default TimestampManagementService timestampManagementService() {
        return new RemoteTimestampManagementAdapter(
                proxyFactory().createProxy(TimestampManagementRpcClient.class, proxyMode()), namespace());
    }

    default long getFreshTimestamp() {
        return timelockService().getFreshTimestamp();
    }

    default TimestampRange getFreshTimestamps(int number) {
        return timelockService().getFreshTimestamps(number);
    }

    default LockResponse lock(LockRequest requestV2) {
        return timelockService().lock(requestV2);
    }

    default boolean unlock(LockToken token) {
        return timelockService().unlock(ImmutableSet.of(token)).contains(token);
    }

    default boolean refreshLockLease(LockToken token) {
        return timelockService().refreshLockLeases(ImmutableSet.of(token)).contains(token);
    }

    default WaitForLocksResponse waitForLocks(WaitForLocksRequest request) {
        return timelockService().waitForLocks(request);
    }
}
