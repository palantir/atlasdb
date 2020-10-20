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
package com.palantir.atlasdb.cli.runner;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

public final class StandardStreamUtilities {
    private StandardStreamUtilities() {
        // Utility class
    }

    private interface StandardStreamSetter {
        void set(PrintStream ps);
    }

    private static String wrapGenericStream(
            Runnable runnable, PrintStream original, StandardStreamSetter standardStreamSetter, boolean singleLine) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        standardStreamSetter.set(new PrintStream(baos));
        try {
            runnable.run();
        } finally {
            standardStreamSetter.set(original);
        }

        return singleLine ? baos.toString().replace("\n", " ").replace("\r", " ") : baos.toString();
    }

    public static String wrapSystemOut(Runnable runnable, boolean singleLine) {
        return wrapGenericStream(runnable, System.out, System::setOut, singleLine);
    }

    public static String wrapSystemOut(Runnable runnable) {
        return wrapSystemOut(runnable, true);
    }

    public static String wrapSystemErr(Runnable runnable, boolean singleLine) {
        return wrapGenericStream(runnable, System.err, System::setErr, singleLine);
    }

    public static String wrapSystemErr(Runnable runnable) {
        return wrapSystemErr(runnable, true);
    }
}
