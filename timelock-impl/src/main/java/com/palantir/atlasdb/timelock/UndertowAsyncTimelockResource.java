/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.conjure.java.undertow.annotations.Handle;
import com.palantir.conjure.java.undertow.annotations.HttpMethod;
import com.palantir.logsafe.Safe;

public final class UndertowAsyncTimelockResource {
    private final TimelockNamespaces namespaces;

    public UndertowAsyncTimelockResource(TimelockNamespaces namespaces) {
        this.namespaces = namespaces;
    }

    @Handle(method = HttpMethod.POST, path = "/{namespace}/timelock/fresh-timestamp")
    public ListenableFuture<Long> getFreshTimestamp(@Safe @Handle.PathParam String namespace) {
        return namespaces.get(namespace).getTimelockService().getFreshTimestampAsync();
    }


}
