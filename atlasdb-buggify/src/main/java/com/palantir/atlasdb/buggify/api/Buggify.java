/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.buggify.api;

/**
 * Instances of this class could vary in implementation, such that they may perform no action {@link com.palantir.atlasdb.buggify.impl.NoOpBuggify} or
 * always perform the action {@link com.palantir.atlasdb.buggify.impl.DefaultBuggify}. The idea here is that a
 * downstream users of this class can easily swap out their {@link BuggifyFactory} to change the behavior of their
 * class, without having to change the implementation of their class. This is useful for tests, as we can force the
 * behavior of never running the provided runnable, or always running the provided runnable.
 *
 * See https://apple.github.io/foundationdb/client-testing.html for more information.
 */
public interface Buggify {
    /**
     * Maybe runs the provided runnable, depending on the instance of the class.
     */
    void run(Runnable runnable);

    /**
     * Returns true if this instance of buggify would perform an action if {@link #run Run} would execute the
     * provided runnable. This is useful if you wish to produce a boolean that will be skewed towards true or false.
     */
    boolean asBoolean();
}
