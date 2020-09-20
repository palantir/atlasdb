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

package com.palantir.paxos.history.models;

import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;

import org.immutables.value.Value;

import com.palantir.common.persist.Persistable;
import com.palantir.paxos.Client;
import com.palantir.paxos.Versionable;

@Value.Immutable
public interface PaxosStateHistory<V extends Persistable & Versionable> {
    @Value.Parameter
    Client namespace();

    @Value.Parameter
    String useCase();

    @Value.Parameter
    List<ConcurrentSkipListMap<Long, V>> localAndRemoteRecords();
}
