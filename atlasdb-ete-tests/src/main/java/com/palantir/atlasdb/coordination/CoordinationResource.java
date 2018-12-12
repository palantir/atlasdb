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

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/coordination")
public interface CoordinationResource {
    @POST
    @Path("/get-transactions-schema-version")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    int getTransactionsSchemaVersion(long timestamp);

    @POST
    @Path("/try-install-transactions-schema-version")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    boolean tryInstallNewTransactionsSchemaVersion(int newVersion);

    @POST
    @Path("/force-install-transactions-schema-version")
    @Consumes(MediaType.APPLICATION_JSON)
    void forceInstallNewTransactionsSchemaVersion(int newVersion);

    @POST
    @Path("/transaction")
    @Produces(MediaType.APPLICATION_JSON)
    boolean doTransactionAndReportOutcome();

    @POST
    @Path("/reset-state")
    @Produces(MediaType.APPLICATION_JSON)
    long resetStateAndGetFreshTimestamp();
}
