/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.asts;

import com.palantir.atlasdb.sweep.asts.SweepStateCoordinator.SweepableBucket;
import com.palantir.refreshable.Disposable;
import java.util.Set;
import java.util.function.Consumer;

// A push model - decouples asking for updates from the logic of debouncing requests.
// Simplifies the coordinator, since it can just repeatedly _ask_ for updates whenever it thinks it's useful to do so
// but we can rate limit it down.
public interface CandidateSweepableBucketRetriever {
    /**
     * Requests an update to the set of buckets. This method will return immediately. There is no guarantee that
     * a refresh will occur after the method is called, as this is simply a hint.
     */
    void requestUpdate();

    /**
     * Registers a callback that will execute after each refresh, including whenever the set of buckets remains constant
     * across refreshes.
     * <p>
     * Callbacks will be executed in the order they are registered. If a callback fails, subsequent callbacks will not
     * be executed. If a callback is slow, it will block subsequent callbacks, and may result in subsequent refreshes
     * being delayed.
     */
    Disposable subscribeToChanges(Consumer<Set<SweepableBucket>> task);
}
