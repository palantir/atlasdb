/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.coordination;

import java.util.concurrent.atomic.AtomicReference;

import javax.ws.rs.Path;

import com.palantir.atlasdb.transaction.api.TransactionManager;

/**
 * This class is necessary to invalidate all underlying caching layers of a {@link CoordinationResource} when
 * performing {@link CoordinationResource#resetStateAndGetFreshTimestamp()}.
 */
@Path("/coordination")
public final class InstanceManagingCoordinationResource implements AutoDelegate_CoordinationResource {
    private final AtomicReference<CoordinationResource> currentDelegate;
    private final TransactionManager txManager;

    private InstanceManagingCoordinationResource(
            AtomicReference<CoordinationResource> currentDelegate,
            TransactionManager txManager) {
        this.currentDelegate = currentDelegate;
        this.txManager = txManager;
    }

    public static CoordinationResource create(TransactionManager txManager) {
        return new InstanceManagingCoordinationResource(
                new AtomicReference<>(SimpleCoordinationResource.create(txManager)),
                txManager);
    }

    @Override
    public CoordinationResource delegate() {
        return currentDelegate.get();
    }

    @Override
    public long resetStateAndGetFreshTimestamp() {
        long freshTimestamp = currentDelegate.get().resetStateAndGetFreshTimestamp();
        currentDelegate.set(SimpleCoordinationResource.create(txManager));
        return freshTimestamp;
    }
}
