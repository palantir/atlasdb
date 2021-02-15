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
package com.palantir.atlasdb.timelock.logging;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import io.dropwizard.logging.FileAppenderFactory;
import io.dropwizard.logging.async.AsyncAppenderFactory;
import io.dropwizard.logging.filter.LevelFilterFactory;
import io.dropwizard.logging.layout.LayoutFactory;

/**
 * A wrapper around the default Dropwizard file appender factory, which sets the neverBlock property to true on the
 * appender. This prevents request log rollovers from blocking request threads.
 * TODO(nziebart): remove this when we switch to internal web framework
 */
@JsonTypeName("non-blocking-file")
public class NonBlockingFileAppenderFactory<E extends ch.qos.logback.core.spi.DeferredProcessingAware>
        extends FileAppenderFactory<E> {

    @Override
    public ch.qos.logback.core.Appender<E> build(
            ch.qos.logback.classic.LoggerContext context,
            String applicationName,
            LayoutFactory<E> layoutFactory,
            LevelFilterFactory<E> levelFilterFactory,
            AsyncAppenderFactory<E> asyncAppenderFactory) {
        ch.qos.logback.core.Appender<E> appender =
                super.build(context, applicationName, layoutFactory, levelFilterFactory, asyncAppenderFactory);

        Preconditions.checkState(
                appender instanceof ch.qos.logback.core.AsyncAppenderBase,
                "The Dropwizard logging factory returned an unexpected appender of type " + appender.getClass()
                        + ". NonBlockingFileAppenderFactory requires an async appender to set the neverBlock "
                        + "property.");
        ((ch.qos.logback.core.AsyncAppenderBase) appender).setNeverBlock(true);

        return appender;
    }
}
