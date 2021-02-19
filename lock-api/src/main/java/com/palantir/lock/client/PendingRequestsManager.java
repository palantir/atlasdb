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

package com.palantir.lock.client;

import com.google.common.collect.ImmutableList;
import com.palantir.lock.client.MultiClientBatchingIdentifiedAtlasDbTransactionStarter.SettableResponse;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

class PendingRequestsManager {
    private final Queue<SettableResponse> pendingRequestQueue;
    private List<StartIdentifiedAtlasDbTransactionResponse> responseList;
    private int start;

    PendingRequestsManager(Queue<SettableResponse> pendingRequestQueue) {
        this.pendingRequestQueue = pendingRequestQueue;
        this.responseList = new ArrayList<>();
        this.start = 0;
    }

    public void acceptResponsesAndServeRequestsGreedily(List<StartIdentifiedAtlasDbTransactionResponse> responses) {
        acceptResponses(responses);
        serveRequests();
    }

    private void acceptResponses(List<StartIdentifiedAtlasDbTransactionResponse> responses) {
        // todo this is inefficient
        responseList.addAll(responses);
    }

    private void serveRequests() {
        // todo so ugly
        int end;
        while (!pendingRequestQueue.isEmpty()) {
            int numRequired = pendingRequestQueue.peek().numTransactions();
            if (start + numRequired > responseList.size()) {
                break;
            }
            end = start + numRequired;
            pendingRequestQueue.poll().future().set(ImmutableList.copyOf(responseList.subList(start, end)));
            start = end;
        }
        responseList = responseList.subList(start, responseList.size());
        start = 0;
    }
}
