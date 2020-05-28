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

import java.util.Set;

import org.immutables.value.Value;

import com.palantir.lock.LockDescriptor;

public interface CommitUpdate {
    <T> T accept(Visitor<T> visitor);

    @Value.Immutable
    interface InvalidateAll extends CommitUpdate {
        @Override
        default <T> T accept(Visitor<T> visitor) {
            return visitor.visit(this);
        }
    }

    @Value.Immutable
    interface InvalidateSome extends CommitUpdate {
        Set<LockDescriptor> invalidatedLocks();

        @Override
        default <T> T accept(Visitor<T> visitor) {
            return visitor.visit(this);
        }
    }

    interface Visitor<T> {
        T visit(InvalidateAll update);
        T visit(InvalidateSome update);
    }
}
