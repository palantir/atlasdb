/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.timelock.util;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.base.Throwables;
import com.palantir.conjure.java.api.errors.QosException;
import com.palantir.conjure.java.api.errors.UnknownRemoteException;
import com.palantir.dialogue.DialogueException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;

public final class ExceptionMatchers {

    private ExceptionMatchers() {}

    public static void isRetryableExceptionWhereLeaderCannotBeFound(Throwable throwable) {
        assertThat(throwable)
                .as(
                        "Exception should be retryable when leader is unavailable: %s\n%s",
                        throwable, Throwables.getStackTraceAsString(throwable))
                .isInstanceOfAny(UnknownRemoteException.class, DialogueException.class)
                .rootCause()
                .isInstanceOfAny(
                        QosException.RetryOther.class,
                        IOException.class,
                        SocketException.class,
                        SocketTimeoutException.class,
                        ConnectException.class);
    }
}
