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

import com.palantir.atlasdb.restore.OutputStateLogger;
import com.palantir.logsafe.Arg;
import org.slf4j.Logger;
import org.slf4j.helpers.MessageFormatter;

@SuppressWarnings("Slf4jConstantLogMessage")
public class OutputPrinter implements OutputStateLogger {
    private final Logger logger;

    public OutputPrinter(Logger logger) {
        this.logger = logger;
    }

    @Override
    public void info(String message, Arg<?>... args) {
        String infoMessage = MessageFormatter.arrayFormat(message, args).getMessage();
        logger.info(message, (Object[]) args);
        System.out.println(infoMessage);
    }

    @Override
    public void warn(String message, Arg<?>... args) {
        String warnMessage = MessageFormatter.arrayFormat(message, args).getMessage();
        logger.warn(message, (Object[]) args);
        System.err.println(warnMessage);
    }

    @Override
    public void error(final String message, Arg<?>... args) {
        String errorMessage = MessageFormatter.arrayFormat(message, args).getMessage();
        logger.error(message, (Object[]) args);
        System.err.println(errorMessage);
    }
}
