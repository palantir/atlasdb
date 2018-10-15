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
package com.palantir.atlasdb.timelock.util;

import java.util.Optional;

import org.immutables.value.Value;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.timelock.AsyncTimelockResource;
import com.palantir.lock.v2.TimelockService;

@Value.Immutable
public abstract class AsyncOrLegacyTimelockService {
    @Value.Parameter
    public abstract Optional<AsyncTimelockResource> getAsyncTimelockResource();

    @Value.Parameter
    public abstract Optional<TimelockService> getLegacyTimelockService();

    public static AsyncOrLegacyTimelockService createFromAsyncTimelock(AsyncTimelockResource asyncTimelockResource) {
        return ImmutableAsyncOrLegacyTimelockService.of(Optional.of(asyncTimelockResource), Optional.empty());
    }

    public static AsyncOrLegacyTimelockService createFromLegacyTimelock(TimelockService legacyTimelockService) {
        return ImmutableAsyncOrLegacyTimelockService.of(Optional.empty(), Optional.of(legacyTimelockService));
    }

    @Value.Check
    protected void check() {
        Preconditions.checkState(
                getAsyncTimelockResource().isPresent() || getLegacyTimelockService().isPresent(),
                "Either the async timelock resource or legacy timelock service should be present!");
        Preconditions.checkState(
                !(getAsyncTimelockResource().isPresent() && getLegacyTimelockService().isPresent()),
                "It shouldn't be that both the async and legacy timelock services are available.");
    }

    public Object getPresentService() {
        if (getAsyncTimelockResource().isPresent()) {
            return getAsyncTimelockResource().get();
        }
        // safe because when creating we know there must be 1
        return getLegacyTimelockService().get();
    }
}
