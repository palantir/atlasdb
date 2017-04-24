/**
 * Copyright 2017 Palantir Technologies
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

import static org.assertj.core.api.Assertions.assertThat;

import javax.ws.rs.NotFoundException;

import org.apache.http.HttpStatus;
import org.junit.Test;

import com.palantir.remoting2.errors.RemoteException;
import com.palantir.remoting2.errors.SerializableError;

@SuppressWarnings("ThrowableInstanceNeverThrown") // This test validates properties of runtime exceptions.
public class AtlasDbRemoteExceptionTest {
    private static final String MESSAGE = "Unknown client";
    private static final String UNKNOWN_EXCEPTION_TYPE = "com.palantir.atlasdb.exception.foo.bar.baz.quux";

    @Test
    public void canBeCreatedFromRemoteException() {
        RemoteException remoteException = new RemoteException(
                SerializableError.of(MESSAGE, NotFoundException.class),
                HttpStatus.SC_NOT_FOUND);

        AtlasDbRemoteException atlasDbRemoteException = new AtlasDbRemoteException(remoteException);
        assertThat(atlasDbRemoteException.getStatus()).isEqualTo(HttpStatus.SC_NOT_FOUND);
        assertThat(atlasDbRemoteException.getMessage()).isEqualTo(MESSAGE);
        assertThat(atlasDbRemoteException.getErrorName()).isEqualTo(NotFoundException.class.getCanonicalName());
    }

    @Test
    public void canBeCreatedFromRemoteExceptionWithUnknownExceptionType() {
        RemoteException remoteException = new RemoteException(
                SerializableError.of(MESSAGE, UNKNOWN_EXCEPTION_TYPE),
                HttpStatus.SC_EXPECTATION_FAILED);

        AtlasDbRemoteException atlasDbRemoteException = new AtlasDbRemoteException(remoteException);
        assertThat(atlasDbRemoteException.getStatus()).isEqualTo(HttpStatus.SC_EXPECTATION_FAILED);
        assertThat(atlasDbRemoteException.getMessage()).isEqualTo(MESSAGE);
        assertThat(atlasDbRemoteException.getErrorName()).isEqualTo(UNKNOWN_EXCEPTION_TYPE);
    }
}
