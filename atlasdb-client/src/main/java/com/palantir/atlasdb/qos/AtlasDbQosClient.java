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

package com.palantir.atlasdb.qos;


import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.async.initializer.AsyncInitializer;
import com.palantir.common.base.Throwables;
import com.palantir.processors.AutoDelegate;

@AutoDelegate(typeToExtend = QosClient.class)
public class AtlasDbQosClient implements QosClient {
    @VisibleForTesting
    class InitializingWrapper extends AsyncInitializer implements AutoDelegate_QosClient {
        @Override
        public QosClient delegate() {
            checkInitialized();
            return AtlasDbQosClient.this;
        }

        @Override
        protected void tryInitialize() {
            AtlasDbQosClient.this.tryInitialize();
        }

        @Override
        protected String getInitializingClassName() {
            return QosClient.class.getSimpleName();
        }
    }

    private final InitializingWrapper wrapper = new InitializingWrapper();
    private final QosService qosService;
    private final String clientName;

    private volatile long credits;

    public static QosClient create(QosService qosService,
            ScheduledExecutorService limitRefresher,
            String clientName,
            boolean initializeAsync) {
        AtlasDbQosClient qosClient = new AtlasDbQosClient(qosService, limitRefresher, clientName);
        qosClient.wrapper.initialize(initializeAsync);
        return qosClient.wrapper.isInitialized() ? qosClient : qosClient.wrapper;
    }

    @VisibleForTesting
    AtlasDbQosClient(QosService qosService,
            ScheduledExecutorService limitRefresher,
            String clientName) {
        this.qosService = qosService;
        this.clientName = clientName;

        limitRefresher.scheduleAtFixedRate(() -> {
            try {
                refreshLimit();
            } catch (Exception e) {
                // do nothing
            }
        }, 0L, 60L, TimeUnit.SECONDS);
    }

    @Override
    public boolean isInitialized() {
        return wrapper.isInitialized();
    }

    private void tryInitialize() {
        try {
            refreshLimit();
        } catch (Exception e) {
            throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
        }
    }

    // The KVS layer should call this before every read/write operation
    // Currently all operations are treated equally; each uses up a unit of credits
    @Override
    public void checkLimit() {
        // always return immediately - i.e. no backoff
        // TODO if soft-limited, pause
        // if hard-limited, throw exception
        if (credits > 0) {
            credits--;
        } else {
            // TODO This should be a ThrottleException?
            throw new RuntimeException("Rate limit exceeded");
        }
    }

    private void refreshLimit() {
        credits = qosService.getLimit(clientName);
    }
}
