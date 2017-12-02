/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.queue;

import java.util.Optional;

import com.palantir.atlasdb.transaction.api.Transaction;

public interface OffsetManager {
    QueueQuery translateWriteQuery(Transaction tx);

    Optional<QueueQuery> translateReadQuery(Transaction tx);

    void updateWriteOffsetPast(Transaction tx, byte[] oldOffset);

    void updateReadOffsetPast(Transaction tx, byte[] oldOffset);
}
