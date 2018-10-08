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
package com.palantir.paxos;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.palantir.common.annotation.Immutable;

@Immutable
public class PaxosUpdate implements PaxosResponse {
    private static final long serialVersionUID = 1L;

    final ImmutableCollection<PaxosValue> values;

    public PaxosUpdate(ImmutableList<PaxosValue> values) {
        this.values = values;
    }

    public ImmutableCollection<PaxosValue> getValues() {
        return values;
    }

    @Override
    public boolean isSuccessful() {
        return true;
    }
}
