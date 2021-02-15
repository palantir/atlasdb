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
package com.palantir.util.jmx;

import javax.annotation.Nonnull;

/**
 * This is a simple timer class that is asked to begin a timer and end after the operation is done.
 *
 * This can be useful for logging or monitoring what is happening in critical sections.
 * <p>
 * All implementations must allow a returned {@link TimingState} to be passed to any other thread
 * who calls {@link TimingState#end()}.  This means that all implementations of OperationTimer must allow
 * a call to {@link #begin(String)} on thread A to pass the returned {@link TimingState} to another
 * thread and that thread may call {@link TimingState#end()}
 * <p>
 * If {@link TimingState#end()} is called more than once, the results are undefined.
 */
public interface OperationTimer {
    @Nonnull
    TimingState begin(String operationName);

    interface TimingState {
        void end();

        TimingState NULL = () -> {
            // empty
        };
    }
}
