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

package com.palantir.lock.watch;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.lock.LockDescriptor;

@Value.Immutable
@Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
@JsonSerialize(as = ImmutableLockWatchState.class)
@JsonDeserialize(as = ImmutableLockWatchState.class)
public interface LockWatchState {
    List<DescriptorAndWatch> state();

    static LockWatchState of(Map<LockDescriptor, LockWatch> state) {
        List<DescriptorAndWatch> entries = state.entrySet()
                .stream()
                .map(t -> ImmutableDescriptorAndWatch.builder().descriptor(t.getKey()).watch(t.getValue()).build())
                .collect(Collectors.toList());
        return ImmutableLockWatchState.builder().addAllState(entries).build();
    }

    @Value.Lazy
    default Map<LockDescriptor, LockWatch> asMap() {
        return state().stream()
                .collect(Collectors.toMap(DescriptorAndWatch::descriptor, DescriptorAndWatch::watch));
    }

    @Value.Immutable
    @JsonSerialize(as = ImmutableDescriptorAndWatch.class)
    @JsonDeserialize(as = ImmutableDescriptorAndWatch.class)
    interface DescriptorAndWatch {
        LockDescriptor descriptor();
        LockWatch watch();
    }
}
