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
package com.palantir.atlasdb.cli.output;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.cli.runner.StandardStreamUtilities;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import org.junit.Test;
import org.slf4j.LoggerFactory;

public class OutputPrinterTest {
    private static final OutputPrinter print = new OutputPrinter(LoggerFactory.getLogger(OutputPrinter.class));

    @Test
    public void testInfoPrintingWorksWithSingleReplacement() {
        String systemOut = StandardStreamUtilities.wrapSystemOut(
                () -> print.info("Test this gets {}", SafeArg.of("replaced", "replaced")));
        assertThat(systemOut).contains("Test this gets replaced ");
    }

    @Test
    public void testWarnPrintingWorksWithSingleReplacement() {
        String systemOut = StandardStreamUtilities.wrapSystemErr(
                () -> print.warn("Test this gets {}", UnsafeArg.of("replaced", "replaced")));
        assertThat(systemOut).contains("Test this gets replaced ");
    }

    @Test
    public void testInfoPrintingWorksWithMultipleReplacement() {
        String systemOut = StandardStreamUtilities.wrapSystemOut(
                () -> print.info("Replace {} of {} {}.", SafeArg.of("all", "all"), SafeArg.of("these", "these"),
                        SafeArg.of("fields", "fields")));
        assertThat(systemOut).contains("Replace all of these fields. ");
    }

    @Test
    public void testErrorPrintingWorksWithMultipleReplacement() {
        String systemErr = StandardStreamUtilities.wrapSystemErr(
                () -> print.error("Replace {} of {} {}.",  SafeArg.of("all", "all"), SafeArg.of("these", "these"),
                        SafeArg.of("fields", "fields")));
        assertThat(systemErr).contains("Replace all of these fields. ");
    }
}
