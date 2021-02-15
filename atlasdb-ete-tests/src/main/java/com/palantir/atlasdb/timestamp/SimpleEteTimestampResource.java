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
package com.palantir.atlasdb.timestamp;

import com.palantir.atlasdb.transaction.api.TransactionManager;

public class SimpleEteTimestampResource implements EteTimestampResource {

    private final TransactionManager transactionManager;

    public SimpleEteTimestampResource(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    @Override
    public long getFreshTimestamp() {
        // TODO(jlach): getFreshTimestamp
        return transactionManager.getTimestampService().getFreshTimestamp();
    }

    @Override
    public void fastForwardTimestamp(long currentTimestamp) {
        transactionManager.getTimestampManagementService().fastForwardTimestamp(currentTimestamp);
    }
}
