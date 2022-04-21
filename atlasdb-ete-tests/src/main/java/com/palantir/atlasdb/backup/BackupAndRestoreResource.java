/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.backup;

import com.palantir.atlasdb.backup.api.AtlasService;
import com.palantir.atlasdb.backup.api.CompletedBackup;
import com.palantir.processors.AutoDelegate;
import java.util.Optional;
import java.util.Set;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@AutoDelegate
@Path("/backup")
public interface BackupAndRestoreResource {
    @POST
    @Path("/prepare-backup")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    Set<AtlasService> prepareBackup(Set<AtlasService> atlasServices);

    @POST
    @Path("/complete-backup")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    Set<AtlasService> completeBackup(Set<AtlasService> atlasServices);

    @POST
    @Path("/prepare-restore")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    Set<AtlasService> prepareRestore(Set<RestoreRequestWithId> restoreRequestsWithId);

    @POST
    @Path("/complete-restore")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    Set<AtlasService> completeRestore(Set<RestoreRequestWithId> restoreRequestsWithId);

    @POST
    @Path("/immutable-ts")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    Optional<Long> getStoredImmutableTimestamp(AtlasService atlasService);

    @POST
    @Path("/ff-ts")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    Optional<CompletedBackup> getStoredBackup(AtlasService atlasService);
}
