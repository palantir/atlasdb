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
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.palantir.remoting2.errors.RemoteException;
import com.palantir.remoting2.errors.SerializableStackTraceElement;

/**
 * An AtlasDbRemoteException is a wrapper around a http-remoting RemoteException.
 *
 * This is necessary because we shadow http-remoting to avoid dependency conflicts with AtlasDB clients, which may
 * be using different versions of http-remoting.
 */
public class AtlasDbRemoteException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final RemoteException remoteException;

    public AtlasDbRemoteException(RemoteException remoteException) {
        this.remoteException = remoteException;
    }

    public int getStatus() {
        return remoteException.getStatus();
    }

    @Override
    public String getMessage() {
        return remoteException.getRemoteException().getMessage();
    }

    public String getErrorName() {
        return remoteException.getRemoteException().getErrorName();
    }

    // Cannot be named getStackTrace() because that is a method on RuntimeException that returns a list of 
    // (non-serializable) StackTraceElements.
    @Nullable // For consistency with HTTP-Remoting API
    public List<AtlasDbStackTraceElement> getRemoteStackTrace() {
        List<SerializableStackTraceElement> stackTraceElements = remoteException.getRemoteException().getStackTrace();
        if (stackTraceElements == null) {
            return null;
        }

        return stackTraceElements.stream()
                .map(AtlasDbStackTraceElement::fromSerializableStackTraceElement)
                .collect(Collectors.toList());
    }

    @Override
    public String toString() {
        // Implemented in this way for consistency with http-remoting RemoteException#toString.
        return super.toString() + remoteException.toString();
    }
}
