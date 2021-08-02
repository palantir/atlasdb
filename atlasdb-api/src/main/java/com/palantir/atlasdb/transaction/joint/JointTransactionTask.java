/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.joint;

/**
 * See {@link com.palantir.atlasdb.transaction.api.TransactionTask}. This version takes a joint transaction, from
 * which users can select relevant bits, so that existing code e.g. Atlas codegenned things still work properly.
 *
 * At some point it's probably better to code-gen this class. Not today, though.
 */
public interface JointTransactionTask<T, E extends Exception> {
    T execute(JointTransaction jointTransaction) throws E;
}
