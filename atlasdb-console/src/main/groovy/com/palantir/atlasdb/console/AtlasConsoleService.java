/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.console;

import java.io.IOException;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.palantir.atlasdb.api.TransactionToken;


@Path("/console2")
public interface AtlasConsoleService {

    @POST
    @Path("tables")
    @Produces(MediaType.APPLICATION_JSON)
    String tables() throws IOException;

    @POST
    @Path("metadata")
    @Produces(MediaType.APPLICATION_JSON)
    String getMetadata(@QueryParam("table") String table) throws IOException;

    @POST
    @Path("get-rows/{token}")
    @Produces(MediaType.APPLICATION_JSON)
    String getRows(@PathParam("token") TransactionToken token, @QueryParam("data") String data) throws IOException;

    @POST
    @Path("get-cells/{token}")
    @Produces(MediaType.APPLICATION_JSON)
    String getCells(@PathParam("token") TransactionToken token, @QueryParam("data") String data) throws IOException;

    @POST
    @Path("get-range/{token}")
    @Produces(MediaType.APPLICATION_JSON)
    String getRange(@PathParam("token") TransactionToken token, @QueryParam("data") String data) throws IOException;

    @POST
    @Path("put/{token}")
    @Produces(MediaType.APPLICATION_JSON)
    void put(@PathParam("token") TransactionToken token, @QueryParam("data") String data) throws IOException;


    @POST
    @Path("delete/{token}")
    @Produces(MediaType.APPLICATION_JSON)
    void delete(@PathParam("token") TransactionToken token, @QueryParam("data") String data) throws IOException;


    @POST
    @Path("truncate")
    @Produces(MediaType.APPLICATION_JSON)
    void truncate(@QueryParam("table") String table) throws IOException;


    @GET
    @Path("start-transaction")
    @Produces(MediaType.APPLICATION_JSON)
    TransactionToken startTransaction();

    @POST
    @Path("commit/{token}")
    @Produces(MediaType.APPLICATION_JSON)
    void commit(@PathParam("token") TransactionToken token);

    @POST
    @Path("abort/{token}")
    @Produces(MediaType.APPLICATION_JSON)
    void abort(@PathParam("token") TransactionToken token);
}
