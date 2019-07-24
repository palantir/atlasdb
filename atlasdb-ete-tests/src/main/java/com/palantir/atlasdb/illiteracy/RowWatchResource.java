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

package com.palantir.atlasdb.illiteracy;

import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.palantir.atlasdb.keyvalue.api.TableReference;

@Path("/watch")
public interface RowWatchResource {
    @POST
    @Path("/begin")
    void beginWatching(@QueryParam("key") String key);

    @POST
    @Path("/begin-prefix")
    void beginWatchingPrefix(@QueryParam("prefix") String prefix);

    @POST
    @Path("/end")
    void endWatching(@QueryParam("key") String key);

    @POST
    @Path("/get")
    String get(@QueryParam("key") String key);

    @POST
    @Path("/get-range")
    @Produces(MediaType.APPLICATION_JSON)
    Map<String, String> getRange(@QueryParam("start") String startInclusive, @QueryParam("end") String endExclusive);

    @POST
    @Path("/put")
    @Consumes(MediaType.APPLICATION_JSON)
    void put(@QueryParam("key") String key, StringWrapper value);

    @POST
    @Path("/get-count")
    @Produces(MediaType.APPLICATION_JSON)
    long getGetCount();

    @POST
    @Path("/get-rr-count")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    long getRangeReadCount(TableReference tableReference);

    @POST
    @Path("/reset-get-count")
    void resetGetCount();

    @POST
    @Path("/flush-cache")
    void flushCache();
}
