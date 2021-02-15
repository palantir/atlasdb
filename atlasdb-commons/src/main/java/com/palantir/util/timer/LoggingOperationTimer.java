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
package com.palantir.util.timer;

import com.palantir.util.jmx.OperationTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LoggingOperationTimer implements OperationTimer {
    private final Logger delegate;

    private LoggingOperationTimer(Logger l) {
        delegate = l;
    }

    public Logger getDelegate() {
        return delegate;
    }

    public static LoggingOperationTimer create(Logger log) {
        return new LoggingOperationTimer(log);
    }

    public static LoggingOperationTimer create(Class<?> clazz) {
        return new LoggingOperationTimer(LoggerFactory.getLogger(clazz.getName()));
    }

    public static LoggingOperationTimer create(String categoryName) {
        return new LoggingOperationTimer(LoggerFactory.getLogger(categoryName));
    }

    private final class TimeBegin implements TimingState {
        private final long tBegin = System.currentTimeMillis();
        private final String msg;

        private TimeBegin(String msg) {
            this.msg = msg;
        }

        @Override
        public void end() {
            if (delegate.isTraceEnabled()) {
                delegate.trace("Duration [{}] ms : {}", System.currentTimeMillis() - tBegin, msg);
            }
        }
    }

    @Override
    public TimingState begin(String operationName) {
        return new TimeBegin(operationName);
    }
}
