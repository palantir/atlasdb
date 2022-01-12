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
import java.nio.charset.StandardCharsets;

public final class StandardStreamUtilities {
    private StandardStreamUtilities() {
        // Utility class
    }

    private interface StandardStreamSetter {
        void set(PrintStream ps);
    }

    private static Runnable wrapWithStream(
            Runnable runnable, PrintStream target, PrintStream original, StandardStreamSetter standardStreamSetter) {
        return () -> {
            standardStreamSetter.set(target);
            try {
                runnable.run();
            } finally {
                standardStreamSetter.set(original);
            }
        };
    }

    private static String wrapGenericStream(
            Runnable runnable, PrintStream original, StandardStreamSetter standardStreamSetter, boolean singleLine) {
        InMemoryPrintStream printStream = InMemoryPrintStream.create();
        wrapWithStream(runnable, printStream, original, standardStreamSetter).run();
        return printStream.getResult(singleLine);
    }

    public static String wrapSystemErrAndOut(Runnable runnable, boolean singleLine) {
        InMemoryPrintStream printStream = InMemoryPrintStream.create();
        Runnable stdOut = wrapWithStream(runnable, printStream, System.out, System::setOut);
        Runnable stdErrAndOut = wrapWithStream(stdOut, printStream, System.err, System::setErr);
        stdErrAndOut.run();
        return printStream.getResult(singleLine);
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

    @SuppressWarnings("FilterOutputStreamSlowMultibyteWrite") // false positive fixed in baseline 4.57.0
    static final class InMemoryPrintStream extends PrintStream {
        private final ByteArrayOutputStream byteOutput;

        static InMemoryPrintStream create() {
            return new InMemoryPrintStream(new ByteArrayOutputStream());
        }

        InMemoryPrintStream(ByteArrayOutputStream out) {
            super(out, /* autoFlush= */ true);
            this.byteOutput = out;
        }

        String getResult(boolean singleLine) {
            return singleLine
                    ? byteOutput
                            .toString(StandardCharsets.UTF_8)
                            .replace("\n", " ")
                            .replace("\r", " ")
                    : byteOutput.toString(StandardCharsets.UTF_8);
        }
    }
}
