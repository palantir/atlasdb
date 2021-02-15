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

package com.palantir.timelock.history.remote;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.conjure.java.undertow.lib.UndertowService;
import com.palantir.timelock.history.HistoryQuery;
import com.palantir.timelock.history.LocalHistoryLoader;
import com.palantir.timelock.history.PaxosHistoryOnRemote;
import com.palantir.timelock.history.TimeLockPaxosHistoryProvider;
import com.palantir.timelock.history.TimeLockPaxosHistoryProviderEndpoints;
import com.palantir.timelock.history.UndertowTimeLockPaxosHistoryProvider;
import com.palantir.tokens.auth.AuthHeader;
import java.util.List;

public final class TimeLockPaxosHistoryProviderResource implements UndertowTimeLockPaxosHistoryProvider {
    private LocalHistoryLoader localHistoryLoader;

    private TimeLockPaxosHistoryProviderResource(LocalHistoryLoader localHistoryLoader) {
        this.localHistoryLoader = localHistoryLoader;
    }

    @Override
    public ListenableFuture<PaxosHistoryOnRemote> getPaxosHistory(
            AuthHeader authHeader, List<HistoryQuery> historyQueries) {
        return Futures.immediateFuture(PaxosHistoryOnRemote.of(
                HistoryLoaderAndTransformer.getLogsForHistoryQueries(localHistoryLoader, historyQueries)));
    }

    public static UndertowService undertow(LocalHistoryLoader localHistoryLoader) {
        return TimeLockPaxosHistoryProviderEndpoints.of(new TimeLockPaxosHistoryProviderResource(localHistoryLoader));
    }

    public static TimeLockPaxosHistoryProvider jersey(LocalHistoryLoader localHistoryLoader) {
        return new JerseyAdapter(new TimeLockPaxosHistoryProviderResource(localHistoryLoader));
    }

    public static final class JerseyAdapter implements TimeLockPaxosHistoryProvider {
        private final TimeLockPaxosHistoryProviderResource delegate;

        private JerseyAdapter(TimeLockPaxosHistoryProviderResource delegate) {
            this.delegate = delegate;
        }

        @Override
        public PaxosHistoryOnRemote getPaxosHistory(AuthHeader authHeader, List<HistoryQuery> historyQueries) {
            return AtlasFutures.getUnchecked(delegate.getPaxosHistory(authHeader, historyQueries));
        }
    }
}
