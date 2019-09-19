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
package com.palantir.lock;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.palantir.logsafe.Preconditions;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/**
 * A client of the lock server. Clients who desire reentrancy are required to
 * identify themselves via unique strings (for example, client IDs).
 *
 * @author jtamer
 */
@Immutable public final class LockClient implements Serializable {
    private static final long serialVersionUID = 0xf5637f2c8d7c94bdL;

    /**
     * An anonymous client of the lock server. Anonymous clients cannot acquire
     * locks reentrantly, because the server has no way to know whether the
     * current client is the same one who already holds the lock.
     */
    public static final LockClient ANONYMOUS = new LockClient("");

    private static final String INTERNAL_LOCK_GRANT_CLIENT_ID = "(internal lock grant client)";

    /**
     * This should only be used by the lock service.
     */
    public static final LockClient INTERNAL_LOCK_GRANT_CLIENT = new LockClient(
            INTERNAL_LOCK_GRANT_CLIENT_ID);

    @Nullable private final String clientId;

    /**
     * Returns a {@code LockClient} instance for the given client ID.
     *
     * Returns LockClient.ANONYMOUS if {@code clientId} is {@code null} or
     *         the empty string
     */
    public static LockClient of(String clientId) {
        if (Strings.isNullOrEmpty(clientId)) {
            return ANONYMOUS;
        }
        Preconditions.checkArgument(!clientId.equals(INTERNAL_LOCK_GRANT_CLIENT_ID));
        return new LockClient(clientId);
    }

    // XXX ONLY use this for deserialization!
    @JsonCreator
    public LockClient(@JsonProperty("clientId") @Nullable String clientId) {
        this.clientId = clientId;
    }

    /** Returns {@code true} if this is an anonymous lock client. */
    @JsonIgnore
    public boolean isAnonymous() {
        return clientId.isEmpty();
    }

    /** Returns the client ID, or the empty string if this is an anonymous client. */
    public String getClientId() {
        return clientId;
    }

    @Override public String toString() {
        return clientId;
    }

    @Override public boolean equals(@Nullable Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof LockClient)) {
            return false;
        }
        return Objects.equal(clientId, ((LockClient) obj).clientId);
    }

    @Override public int hashCode() {
        return Objects.hashCode(clientId);
    }

    private void readObject(@SuppressWarnings("unused") ObjectInputStream in)
            throws InvalidObjectException {
        throw new InvalidObjectException("proxy required");
    }

    private Object writeReplace() {
        return new SerializationProxy(this);
    }

    private static class SerializationProxy implements Serializable {
        private static final long serialVersionUID = 0x495befe620789284L;

        @Nullable private final String clientId;

        SerializationProxy(LockClient lockClient) {
            clientId = lockClient.clientId;
        }

        Object readResolve() {
            if (Strings.isNullOrEmpty(clientId)) {
                return ANONYMOUS;
            }
            if (clientId.equals(INTERNAL_LOCK_GRANT_CLIENT_ID)) {
                return INTERNAL_LOCK_GRANT_CLIENT;
            }
            return of(clientId);
        }
    }
}
