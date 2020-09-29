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
package com.palantir.atlasdb.jepsen.utils;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.jepsen.CheckerResult;
import com.palantir.atlasdb.jepsen.events.Checker;
import com.palantir.atlasdb.jepsen.events.Event;
import java.util.List;
import java.util.function.Supplier;

public final class CheckerTestUtils {
    private CheckerTestUtils() {
        // utility class
    }

    public static void assertNoErrors(Supplier<Checker> checker, Event... events) {
        assertNoErrors(checker, ImmutableList.copyOf(events));
    }

    public static void assertNoErrors(Supplier<Checker> checker, List<Event> events) {
        CheckerResult result = checker.get().check(events);

        assertThat(result.valid()).isTrue();
        assertThat(result.errors()).isEmpty();
    }
}
