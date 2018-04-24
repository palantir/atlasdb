/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.http.errors;

import java.util.List;

import javax.ws.rs.NotFoundException;

import org.apache.http.HttpStatus;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.palantir.remoting2.errors.RemoteException;
import com.palantir.remoting2.errors.SerializableError;
import com.palantir.remoting2.errors.SerializableStackTraceElement;

@SuppressWarnings("ThrowableInstanceNeverThrown") // This test validates properties of runtime exceptions.
public class AtlasDbRemoteExceptionTest {
    private static final String MESSAGE = "Unknown client";
    private static final String UNKNOWN_EXCEPTION_TYPE = "com.palantir.atlasdb.exception.foo.bar.baz.quux";

    private static final List<SerializableStackTraceElement> STACK_TRACE = ImmutableList.of(
            getStackTraceElement("foo", "bar"),
            getStackTraceElement("baz", "quux")
    );

    @Test
    public void canBeCreatedFromRemoteExceptionWithoutStackTrace() {
        RemoteException remoteException = new RemoteException(
                SerializableError.of(MESSAGE, NotFoundException.class),
                HttpStatus.SC_NOT_FOUND);

        assertCreatedExceptionMatches(remoteException);
    }

    @Test
    public void canBeCreatedFromRemoteExceptionWithUnknownExceptionType() {
        RemoteException remoteException = new RemoteException(
                SerializableError.of(MESSAGE, UNKNOWN_EXCEPTION_TYPE),
                HttpStatus.SC_EXPECTATION_FAILED);

        assertCreatedExceptionMatches(remoteException);
    }

    @Test
    public void canBeCreatedFromRemoteExceptionWithStackTrace() {
        RemoteException remoteException = new RemoteException(
                SerializableError.withCustomStackTrace(MESSAGE, NotFoundException.class, STACK_TRACE),
                HttpStatus.SC_NOT_FOUND);

        assertCreatedExceptionMatches(remoteException);
    }

    private static void assertCreatedExceptionMatches(RemoteException remoteException) {
        AtlasDbRemoteException atlasDbRemoteException = new AtlasDbRemoteException(remoteException);
        RemotingExceptionTestUtils.assertRemoteExceptionsMatch(atlasDbRemoteException, remoteException);
    }

    private static SerializableStackTraceElement getStackTraceElement(String className, String methodName) {
        return SerializableStackTraceElement.builder()
                .className(className)
                .methodName(methodName)
                .build();
    }
}
