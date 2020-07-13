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
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.palantir.timelock.feedback.ConjureTimeLockClientFeedback;

public class ServiceFeedback {

    private Map<UUID, List<ConjureTimeLockClientFeedback>> nodeWiseFeedback = Maps.newConcurrentMap();

    public void addFeedbackForNode(UUID nodeId, ConjureTimeLockClientFeedback feedback) {
        nodeWiseFeedback.computeIfAbsent(nodeId, node -> Lists.newArrayList()).add(feedback);
    }

    public Collection<List<ConjureTimeLockClientFeedback>> values() {
        return nodeWiseFeedback.values();
    }

    public int numberOfNodes() {
        return nodeWiseFeedback.size();
    }
}
