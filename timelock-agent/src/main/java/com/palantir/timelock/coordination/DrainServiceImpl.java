/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.timelock.coordination;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;

import javax.ws.rs.BadRequestException;

import com.google.common.collect.Maps;
import com.palantir.atlasdb.timelock.TimeLockServices;
import com.palantir.leader.Drainable;

public class DrainServiceImpl implements DrainService {
    private final Map<String, Function<HostTransition, TimeLockServices>> serviceSuppliers;
    private final Map<String, TimeLockServices> actualServices;
    private final BiConsumer<String, HostTransition> regenCallback;

    public DrainServiceImpl(Map<String, TimeLockServices> actualServices,
            BiConsumer<String, HostTransition> regenCallback) {
        this.serviceSuppliers = Maps.newConcurrentMap();
        this.actualServices = actualServices;
        this.regenCallback = regenCallback;
    }

    public void register(String client, Function<HostTransition, TimeLockServices> timeLockServices) {
        serviceSuppliers.put(client, timeLockServices);
    }

    @Override
    public synchronized void drain(String client) {
        if (!serviceSuppliers.containsKey(client)) {
            throw new BadRequestException("Client " + client + " not defined!");
        }
        TimeLockServices timeLockServices = actualServices.get(client);
        CompletableFuture<Void> future = new CompletableFuture<>();
        future.complete(null);
        if (timeLockServices.getTimestampManagementService() instanceof Drainable) {
            future.thenCompose(unused -> ((Drainable) timeLockServices.getTimestampManagementService()).drain());
        }
        if (timeLockServices.getLockService() instanceof Drainable) {
            future.thenCompose(unused -> ((Drainable) timeLockServices.getLockService()).drain());
        }
        if (timeLockServices.getTimelockService() instanceof Drainable) {
            future.thenCompose(unused -> ((Drainable) timeLockServices.getTimelockService()).drain());
        }
        future.join();
    }

    @Override
    public synchronized void regenerate(String client, HostTransition hostsForClient) {
        if (!serviceSuppliers.containsKey(client)) {
            throw new BadRequestException("Client " + client + " not defined!");
        }
        TimeLockServices newServices = serviceSuppliers.get(client).apply(hostsForClient);
        actualServices.put(client, newServices);
        regenCallback.accept(client, hostsForClient);
    }
}
