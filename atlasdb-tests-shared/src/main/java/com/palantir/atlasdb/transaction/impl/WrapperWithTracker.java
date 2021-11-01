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

package com.palantir.atlasdb.transaction.impl;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.api.Transaction;

interface WrapperWithTracker<T> {
    WrapperWithTracker<Transaction> TRANSACTION_NO_OP = (delegate, _synchronousTracker) -> delegate;

    WrapperWithTracker<KeyValueService> KEY_VALUE_SERVICE_NO_OP = (delegate, _synchronousTracker) -> delegate;

    T apply(T delegate, PathTypeTracker pathTypeTracker);
}
