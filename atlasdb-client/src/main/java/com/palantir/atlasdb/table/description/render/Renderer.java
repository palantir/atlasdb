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
package com.palantir.atlasdb.table.description.render;

import com.google.common.hash.Hashing;
import com.palantir.logsafe.Preconditions;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public abstract class Renderer {
    private final StringBuilder builder;
    private final AtomicInteger indent;

    public Renderer() {
        this.builder = new StringBuilder();
        this.indent = new AtomicInteger();
    }

    public Renderer(Renderer parent) {
        this.builder = parent.builder;
        this.indent = parent.indent;
    }

    protected abstract void run();

    protected String render() {
        Preconditions.checkState(builder.length() == 0);
        run();
        return builder.toString();
    }

    protected void line() {
        builder.append("\n");
    }

    protected void line(String... strings) {
        if (builder.length() != 0) {
            builder.append("\n");
        }
        if (strings[0].startsWith("}")) {
            indent.decrementAndGet();
        }
        for (int i = 0; i < indent.get(); i++) {
            builder.append("    ");
        }
        lineEnd(strings);
    }

    protected void lineEnd(String... strings) {
        for (String string : strings) {
            builder.append(string);
        }
        if (strings[strings.length - 1].endsWith("{")) {
            indent.incrementAndGet();
        }
    }

    protected void strip(String suffix) {
        if (builder.lastIndexOf(suffix) + suffix.length() == builder.length()) {
            builder.delete(builder.length() - suffix.length(), builder.length());
        }
    }

    protected void replace(String suffix, String replacement) {
        strip(suffix);
        lineEnd(replacement);
    }

    protected byte[] getHash() {
        return Hashing.murmur3_128().hashUnencodedChars(builder).asBytes();
    }
}
