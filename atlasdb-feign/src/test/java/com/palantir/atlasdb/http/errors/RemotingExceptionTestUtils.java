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
package com.palantir.atlasdb.http.errors;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.IntStream;

import com.palantir.remoting2.errors.RemoteException;
import com.palantir.remoting2.errors.SerializableStackTraceElement;

public final class RemotingExceptionTestUtils {
    private RemotingExceptionTestUtils() {
        // utility
    }

    public static void assertStackTraceElementsMatch(
            AtlasDbStackTraceElement atlasDbElement,
            SerializableStackTraceElement httpRemotingElement) {
        assertThat(atlasDbElement.getClassName()).isEqualTo(httpRemotingElement.getClassName());
        assertThat(atlasDbElement.getMethodName()).isEqualTo(httpRemotingElement.getMethodName());
        assertThat(atlasDbElement.getFileName()).isEqualTo(httpRemotingElement.getFileName());
        assertThat(atlasDbElement.getLineNumber()).isEqualTo(httpRemotingElement.getLineNumber());
    }

    public static void assertRemoteExceptionsMatch(
            AtlasDbRemoteException atlasDbRemoteException,
            RemoteException httpRemotingRemoteException) {
        assertThat(atlasDbRemoteException.getStatus()).isEqualTo(httpRemotingRemoteException.getStatus());
        assertThat(atlasDbRemoteException.getMessage())
                .isEqualTo(httpRemotingRemoteException.getRemoteException().getMessage());
        assertThat(atlasDbRemoteException.getErrorName())
                .isEqualTo(httpRemotingRemoteException.getRemoteException().getErrorName());


        assertThat(atlasDbRemoteException.getRemoteStackTrace())
                .satisfies(stackTraceElements -> {
                    List<SerializableStackTraceElement> httpRemotingStackTraceElements
                            = httpRemotingRemoteException.getRemoteException().getStackTrace();

                    if (httpRemotingStackTraceElements == null) {
                        assertThat(stackTraceElements).isNull();
                        return;
                    }

                    assertThat(stackTraceElements).isNotNull();
                    assertThat(stackTraceElements.size()).isEqualTo(httpRemotingStackTraceElements.size());

                    IntStream.range(0, stackTraceElements.size())
                            .forEach(index -> assertStackTraceElementsMatch(
                                    stackTraceElements.get(index),
                                    httpRemotingStackTraceElements.get(index)));
                });
    }
}
