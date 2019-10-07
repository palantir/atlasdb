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

package com.palantir.common.concurrent;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.junit.Test;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName") // Name matches the class we're testing
public class PTExecutorsTest {

    @Test
    public void testExecutorName_namedThreadFactory() {
        ThreadFactory factory = new NamedThreadFactory("my-prefix");
        assertThat(PTExecutors.getExecutorName(factory)).isEqualTo("my-prefix");
    }

    @Test
    public void testExecutorName_customThreadFactory() {
        ThreadFactory factory = runnable -> {
            Thread thread = Executors.defaultThreadFactory().newThread(runnable);
            thread.setName("foo-3");
            return thread;
        };
        assertThat(PTExecutors.getExecutorName(factory)).isEqualTo("foo");
    }

    @Test
    public void testExecutorName_customThreadFactory_fallback() {
        ThreadFactory factory = runnable -> {
            Thread thread = Executors.defaultThreadFactory().newThread(runnable);
            thread.setName("1");
            return thread;
        };
        assertThat(PTExecutors.getExecutorName(factory)).isEqualTo("PTExecutor");
    }
}
