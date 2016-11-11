/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.cli.output;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.junit.Test;
import org.slf4j.LoggerFactory;

public class OutputPrinterTest {
    private static final OutputPrinter print = new OutputPrinter(LoggerFactory.getLogger(OutputPrinterTest.class));

    private interface StandardStreamSetter {
        void set(PrintStream ps);
    }

    private String wrapGenericStream(Runnable runnable, PrintStream original, StandardStreamSetter standardStreamSetter) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        standardStreamSetter.set(new PrintStream(baos));
        try {
            runnable.run();
        } finally {
            standardStreamSetter.set(original);
        }
        return baos.toString().replace("\n", " ").replace("\r", " ");
    }

    private String wrapSystemOut(Runnable runnable) {
        return wrapGenericStream(runnable, System.out, System::setOut);
    }

    private String wrapSystemErr(Runnable runnable) {
        return wrapGenericStream(runnable, System.err, System::setErr);
    }

    @Test
    public void testInfoPrintingWorksWithSingleReplacement() {
        String systemOut = wrapSystemOut(() -> print.info("Test this gets {}", "replaced"));
        assertThat(systemOut).isEqualTo("Test this gets replaced ");
    }

    @Test
    public void testInfoPrintingWorksWithMultipleReplacement() {
        String systemOut = wrapSystemOut(() -> print.info("Replace {} of {} {}.", "all", "these", "fields"));
        assertThat(systemOut).isEqualTo("Replace all of these fields. ");
    }

    @Test
    public void testErrorPrintingWorksWithMultipleReplacement() {
        String systemErr = wrapSystemErr(() -> print.error("Replace {} of {} {}.", "all", "these", "fields"));
        assertThat(systemErr).isEqualTo("Replace all of these fields. ");
    }
}