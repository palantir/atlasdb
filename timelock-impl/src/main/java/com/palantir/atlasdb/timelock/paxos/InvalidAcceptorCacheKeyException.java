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

package com.palantir.atlasdb.timelock.paxos;

import com.google.common.collect.ImmutableList;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.SafeLoggable;
import java.util.List;

public class InvalidAcceptorCacheKeyException extends Exception implements SafeLoggable {

    private static final String MESSAGE = "Provided acceptor cache key is either invalid/expired.";

    private final AcceptorCacheKey cacheKey;

    public InvalidAcceptorCacheKeyException(AcceptorCacheKey cacheKey) {
        super(MESSAGE);
        this.cacheKey = cacheKey;
    }

    @Override
    public String getLogMessage() {
        return MESSAGE;
    }

    @Override
    public List<Arg<?>> getArgs() {
        return ImmutableList.of(SafeArg.of("cacheKey", cacheKey));
    }

    public AcceptorCacheKey cacheKey() {
        return cacheKey;
    }
}
