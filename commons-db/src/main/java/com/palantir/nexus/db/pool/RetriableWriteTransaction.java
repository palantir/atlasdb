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
package com.palantir.nexus.db.pool;

import java.sql.Connection;
import java.sql.SQLException;
import javax.annotation.Nullable;

/**
 * A "retriable" write transaction.  For transactions meeting a minimum of
 * criteria it is possible to be safely retried.  Essentially:
 *
 * *) The transaction MAY run whatever perfectly normal CRUD actions it likes.
 * *) The transaction MUST NOT mess with the connection transaction state internally.  Do not call commit(), do not call abort().
 * *) The transaction SHOULD NOT alter external state in any way.  At best this would be very hard to do safely, most likely it will result in incorrect behaviour on transient DB problems.
 * *) The transaction MUST NOT alter external state in a way that needs to atomically match whether or not the transaction succeeds.  See previous.
 * *) The transaction MUST NOT assume that it returning means success (commit could fail).
 * *) The transaction MUST NOT assume that it throwing means "success" (cleanup could fail causing retry, any form of {@link SQLException} will be retried).
 *
 * In general:
 *
 * *) Communicate into your implementation solely via read-only, immutable data.
 * *) Communicate out of your implementation solely by the return value / thrown exception.
 * *) Under normal conditions eventually things will succeed and you'll get either (your return + committed transaction) or (your throw + no committed transaction)
 * *) Under full DB outage you'll eventually get a thrown {@link SQLException}.
 * *) Under sufficiently dire DB conditions (e.g.  DB goes down permanently right as transaction is running) you may get a [unknowingly] committed transaction plus a thrown {@link SQLException} (commit attempted which completes and throws, DB is hosed enough after that to be unable to verify this).
 */
public interface RetriableWriteTransaction<T> {
    public @Nullable T run(Connection c) throws SQLException;
}
