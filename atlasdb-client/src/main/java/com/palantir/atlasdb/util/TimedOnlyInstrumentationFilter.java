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

package com.palantir.atlasdb.util;

import com.palantir.atlasdb.metrics.Timed;
import com.palantir.tritium.api.event.InstrumentationFilter;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

final class TimedOnlyInstrumentationFilter implements InstrumentationFilter {

    private final Map<Method, Boolean> filtered = new ConcurrentHashMap<>();

    @Override
    public boolean shouldInstrument(@Nonnull Object _instance, @Nonnull Method method, @Nonnull Object[] _args) {
        return filtered.computeIfAbsent(method, this::shouldInstrument);
    }

    private boolean shouldInstrument(Method method) {
        return method.getAnnotation(Timed.class) != null;
    }
}
