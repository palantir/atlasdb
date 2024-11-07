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

package com.palantir.atlasdb.transaction.api;

import java.io.Closeable;

public interface OpenTransaction extends Transaction, Closeable {
    /**
     * Aborts the transaction if uncommitted and cleanups transaction state.
     * All open transactions <b>must be closed</b>.
     * Not closing transactions after they're no longer in use may lead to arbitrary delays elsewhere in the system.
     */
    @Override
    void close();
}
