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
package com.palantir.exception;

import com.palantir.common.exception.AtlasDbDependencyException;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.Safe;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.SafeLoggable;
import java.util.List;

public class NotInitializedException extends AtlasDbDependencyException implements SafeLoggable {

    private static final String EXCEPTION_MESSAGE =
            "The object is not yet initialized, and as a result, all calls to it will fail. Interactions with this"
                    + " object should not take place until after initialization has completed. For example, if your"
                    + " service that handles REST requests relies on this object, it should not service REST requests"
                    + " until initialization has completed, as otherwise all calls are guaranteed to fail. It is"
                    + " expected that initialization of some object types do take some time.";
    private final String objectNotInitialized;

    public NotInitializedException(@Safe String objectNotInitialized) {
        super(EXCEPTION_MESSAGE);
        this.objectNotInitialized = objectNotInitialized;
    }

    public NotInitializedException(@Safe String objectNotInitialized, Throwable throwable) {
        super(EXCEPTION_MESSAGE, throwable);
        this.objectNotInitialized = objectNotInitialized;
    }

    @Override
    @Safe
    public String getLogMessage() {
        return EXCEPTION_MESSAGE;
    }

    @Override
    public List<Arg<?>> getArgs() {
        return List.of(SafeArg.of("objectName", objectNotInitialized));
    }
}
