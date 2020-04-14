/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.management;

import java.nio.file.Path;
import java.util.Set;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.timelock.api.TimeLockManagementService;
import com.palantir.atlasdb.timelock.api.TimeLockManagementServiceEndpoints;
import com.palantir.atlasdb.timelock.api.UndertowTimeLockManagementService;
import com.palantir.conjure.java.undertow.lib.UndertowService;
import com.palantir.tokens.auth.AuthHeader;

public class TimeLockManagementResource implements UndertowTimeLockManagementService {
    private final DiskNamespaceLoader diskNamespaceLoader;

    private TimeLockManagementResource(DiskNamespaceLoader diskNamespaceLoader) {
        this.diskNamespaceLoader = diskNamespaceLoader;
    }

    public static TimeLockManagementResource create(Path rootDataDirectory) {
        return new TimeLockManagementResource(new DiskNamespaceLoader(rootDataDirectory));
    }

    public static UndertowService undertow(Path rootDataDirectory) {
        return TimeLockManagementServiceEndpoints.of(TimeLockManagementResource.create(rootDataDirectory));
    }

    public static TimeLockManagementService jersey(Path rootDataDirectory) {
        return new JerseyAdapter(TimeLockManagementResource.create(rootDataDirectory));
    }

    @Override
    public ListenableFuture<Set<String>> getNamespaces(AuthHeader authHeader) {
        // This endpoint is not used frequently (only called by migration cli), so I'm ok with this NOT being async.
        return Futures.immediateFuture(diskNamespaceLoader.getNamespaces());
    }

    public static final class JerseyAdapter implements TimeLockManagementService {
        private final TimeLockManagementResource resource;

        private JerseyAdapter(TimeLockManagementResource resource) {
            this.resource = resource;
        }

        @Override
        public Set<String> getNamespaces(AuthHeader authHeader) {
            return unwrap(resource.getNamespaces(authHeader));
        }

        private static <T> T unwrap(ListenableFuture<T> future) {
            return AtlasFutures.getUnchecked(future);
        }
    }
}
