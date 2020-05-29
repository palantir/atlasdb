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

package com.palantir.lock.watch;

import java.util.List;

import org.immutables.value.Value;

public interface ClientEventUpdate {
    <T> T accept(Visitor<T> visitor);

    interface Visitor<T> {
        T visit(Events events);
        T visit(Snapshot snapshot);
    }

    static Events success(List<LockWatchEvent> events) {
        return ImmutableEvents.of(events);
    }

    static Snapshot failure(LockWatchStateUpdate.Snapshot snapshot) {
        return ImmutableSnapshot.of(snapshot);
    }

    @Value.Immutable
    interface Events extends ClientEventUpdate {
        @Value.Parameter
        List<LockWatchEvent> events();

        @Override
        default <T> T accept(Visitor<T> visitor) {
            return visitor.visit(this);
        }
    }

    @Value.Immutable
    interface Snapshot extends ClientEventUpdate {
        @Value.Parameter
        LockWatchStateUpdate.Snapshot snapshot();

        @Override
        default <T> T accept(Visitor<T> visitor) {
            return visitor.visit(this);
        }
    }
}
