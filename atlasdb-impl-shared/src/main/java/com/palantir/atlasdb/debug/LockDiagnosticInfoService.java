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

package com.palantir.atlasdb.debug;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * TODO(fdesouza): Remove this once PDS-95791 is resolved.
 * @deprecated Remove this once PDS-95791 is resolved.
 */
@Deprecated
@Path("/{namespace}/timelock")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public interface LockDiagnosticInfoService {

    @POST
    @Path("do-not-use-without-explicit-atlasdb-authorisation/lock-diagnostic-config")
    Optional<LockDiagnosticInfo> getEnhancedLockDiagnosticInfo(
            @PathParam("namespace") String namespace, Set<UUID> requestIds);
}
