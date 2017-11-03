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


import javax.naming.LimitExceededException;

// TODO this class should use a QosService and update limits
public final class AtlasDbQosClient {
    QosServiceResource qosService;

    volatile int credits;

    private AtlasDbQosClient(QosServiceResource qosService) {
        this.qosService = qosService;
    }

    public static AtlasDbQosClient create(QosServiceResource qosService,
            String clientName) {
        AtlasDbQosClient client = new AtlasDbQosClient(qosService);
        client.credits = qosService.getLimit(clientName);
        return client;
    }

    // The KVS layer should call this before every read/write operation
    // Currently all operations are treated equally; each uses up a unit of credits
    // TODO use a background thread to refresh this limit
    public void checkLimit() throws LimitExceededException {
        // always return immediately - i.e. no backoff
        // TODO if soft-limited, pause
        // if hard-limited, throw exception
        if (credits > 0) {
            credits--;
        } else {
            // TODO is there a more appropriate exception?
            throw new LimitExceededException("Rate limit exceeded");
        }
    }
}
