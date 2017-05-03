/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.table.description.render;

import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Preconditions;
import com.google.common.hash.Hashing;

@NotThreadSafe
public abstract class Renderer {
    private final StringBuilder s;
    private final AtomicInteger indent;

    public Renderer() {
        this.s = new StringBuilder();
        this.indent = new AtomicInteger();
    }

    public Renderer(Renderer parent) {
        this.s = parent.s;
        this.indent = parent.indent;
    }

    protected abstract void run();

    protected String render() {
        Preconditions.checkState(s.length() == 0);
        run();
        return s.toString();
    }

    protected void line() {
        s.append("\n");
    }

    protected void line(String... strings) {
        if (s.length() != 0) {
            s.append("\n");
        }
        if (strings[0].startsWith("}")) {
            indent.decrementAndGet();
        }
        for (int i = 0; i < indent.get(); i++) {
            s.append("    ");
        }
        lineEnd(strings);
    }

    protected void lineEnd(String... strings) {
        for (String string : strings) {
            s.append(string);
        }
        if (strings[strings.length - 1].endsWith("{")) {
            indent.incrementAndGet();
        }
    }

    protected void strip(String suffix) {
        if (s.lastIndexOf(suffix) + suffix.length() == s.length()) {
            s.delete(s.length() - suffix.length(), s.length());
        }
    }

    protected void replace(String suffix, String replacement) {
        strip(suffix);
        lineEnd(replacement);
    }

    protected byte[] getHash() {
        return Hashing.murmur3_128().hashUnencodedChars(s).asBytes();
    }
}
