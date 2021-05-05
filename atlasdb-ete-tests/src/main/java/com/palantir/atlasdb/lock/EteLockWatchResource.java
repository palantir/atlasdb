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

package com.palantir.atlasdb.lock;

import com.palantir.atlasdb.keyvalue.api.cache.HitDigest;
import com.palantir.lock.watch.CommitUpdate;
import com.palantir.lock.watch.TransactionsLockWatchUpdate;
import java.util.Optional;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("lock-watch")
public interface EteLockWatchResource {
    @POST
    @Path("start-transaction")
    @Produces(MediaType.APPLICATION_JSON)
    TransactionId startTransaction();

    @POST
    @Path("end-transaction")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    Optional<CommitUpdate> endTransaction(TransactionId transactionId);

    @POST
    @Path("end-transaction-hit-digest")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    HitDigest endTransactionWithDigest(TransactionId transactionId);

    @POST
    @Path("write")
    @Consumes(MediaType.APPLICATION_JSON)
    void write(WriteRequest writeRequest);

    @POST
    @Path("read")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    ReadResponse read(ReadRequest readRequest);

    @POST
    @Path("get-update")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    TransactionsLockWatchUpdate getUpdate(GetLockWatchUpdateRequest updateRequest);

    @POST
    @Path("set-table")
    @Consumes(MediaType.TEXT_PLAIN)
    void setTable(String tableName);
}
