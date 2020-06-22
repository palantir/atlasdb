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

package com.palantir.atlasdb.timelock.adjudicate;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.EvictingQueue;
import com.google.common.collect.ImmutableMap;
import com.palantir.timelock.feedback.ConjureTimeLockClientFeedback;

public class FeedbackReportsSink {
    private FeedbackReportsSink() {
        // no op
    }

    private static Cache<String, ServiceHealthTracker.Service> trackedServices = Caffeine.newBuilder()
            .maximumSize(10_000)
            .expireAfterAccess(2, TimeUnit.MINUTES)
            .build();

    public static void registerFeedbackReport(ConjureTimeLockClientFeedback feedback) {
        ServiceHealthTracker.Service service = trackedServices.asMap().getOrDefault(feedback.getServiceName(),
                ImmutableService.builder().serviceName(feedback.getServiceName()).nodes(ImmutableMap.of()).build());
        NodeHealthTracker.Node node = service.nodes().getOrDefault(feedback.getNodeId(),
                ImmutableNode.builder().nodeId(feedback.getNodeId()).reports(EvictingQueue.create(10)).build());
        node.reports().add(feedback);
    }

    public static Collection<ServiceHealthTracker.Service> getTrackedServices() {
        return trackedServices.asMap().values();
    }

}
