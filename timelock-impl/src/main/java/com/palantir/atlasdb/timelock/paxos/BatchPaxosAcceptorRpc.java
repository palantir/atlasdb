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

package com.palantir.atlasdb.timelock.paxos;

import static com.palantir.atlasdb.timelock.paxos.BatchPaxosAcceptorRpcClient.CACHE_KEY_NOT_FOUND;

import com.palantir.conjure.java.api.errors.ServiceException;
import com.palantir.logsafe.SafeArg;

public final class BatchPaxosAcceptorRpc {

    private BatchPaxosAcceptorRpc() {}

    public static final class Errors {
        private Errors() {}

        public static ServiceException invalidCacheKeyException(AcceptorCacheKey cacheKey) {
            return new ServiceException(CACHE_KEY_NOT_FOUND, SafeArg.of("cacheKey", cacheKey));
        }
    }

}
