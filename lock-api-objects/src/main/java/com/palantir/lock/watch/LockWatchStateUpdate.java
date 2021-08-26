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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.common.annotations.ImmutablesStyles.PackageVisibleImmutablesStyle;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.watch.LockWatchReferences.LockWatchReference;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.immutables.value.Value;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = LockWatchStateUpdate.Success.class, name = LockWatchStateUpdate.Success.TYPE),
    @JsonSubTypes.Type(value = LockWatchStateUpdate.Snapshot.class, name = LockWatchStateUpdate.Snapshot.TYPE)
})
public interface LockWatchStateUpdate {
    UUID logId();

    <T> T accept(Visitor<T> visitor);

    static Success success(UUID logId, long version, List<LockWatchEvent> events) {
        return ImmutableSuccess.builder()
                .logId(logId)
                .lastKnownVersion(version)
                .events(events)
                .build();
    }

    static Snapshot snapshot(
            UUID logId, long version, Set<LockDescriptor> locked, Set<LockWatchReference> lockWatches) {
        return ImmutableSnapshot.builder()
                .logId(logId)
                .lastKnownVersion(version)
                .locked(locked)
                .lockWatches(lockWatches)
                .build();
    }

    /**
     * A successful update is an update containing information about all lock watch events occurring since the previous
     * last known version.
     */
    @Value.Immutable
    @PackageVisibleImmutablesStyle
    @JsonSerialize(as = ImmutableSuccess.class)
    @JsonDeserialize(as = ImmutableSuccess.class)
    @JsonTypeName(Success.TYPE)
    interface Success extends LockWatchStateUpdate {
        String TYPE = "success";

        long lastKnownVersion();

        List<LockWatchEvent> events();

        @Override
        default <T> T accept(Visitor<T> visitor) {
            return visitor.visit(this);
        }
    }

    /**
     * A snapshot update is generally returned when it was impossible to return a successful update. This can happen
     * if we fall behind, or we just started so we don't have a last known version. It generally
     * means that all previous lock watch information must be purged, as it is impossible to know what events were
     * missed, but contains all of the current lock watch information as the state of the world moving forward.
     */
    @Value.Immutable
    @PackageVisibleImmutablesStyle
    @JsonSerialize(as = ImmutableSnapshot.class)
    @JsonDeserialize(as = ImmutableSnapshot.class)
    @JsonTypeName(Snapshot.TYPE)
    interface Snapshot extends LockWatchStateUpdate {
        String TYPE = "snapshot";

        long lastKnownVersion();

        Set<LockDescriptor> locked();

        Set<LockWatchReference> lockWatches();

        @Override
        default <T> T accept(Visitor<T> visitor) {
            return visitor.visit(this);
        }
    }

    interface Visitor<T> {
        T visit(Success success);

        T visit(Snapshot snapshot);
    }
}
