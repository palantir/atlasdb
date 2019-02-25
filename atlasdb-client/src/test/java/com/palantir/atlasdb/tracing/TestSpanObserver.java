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
package com.palantir.atlasdb.tracing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.palantir.tracing.api.Span;
import com.palantir.tracing.api.SpanObserver;

public class TestSpanObserver implements SpanObserver {
    private final List<Span> spans = new ArrayList<>();

    @Override
    public void consume(Span span) {
        spans.add(span);
    }

    public List<Span> spans() {
        return Collections.unmodifiableList(spans);
    }
}
