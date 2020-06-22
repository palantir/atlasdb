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
package com.palantir.atlasdb.keyvalue.api;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Collection;

/**
 * A {@link KeyAlreadyExistsException} is thrown if an operation that conditionally updates a {@link KeyValueService}
 * fails because some data is already present in the underlying database.
 */
public class KeyAlreadyExistsException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    /**
     * The {@link Cell}s present in this list contributed to the failure of the conditional update, in that they were
     * already present when the conditional update expected them to not be present. This list may not be complete;
     * there may be additional cells that the conditional update expected to not be present that are actually present.
     */
    private final ImmutableList<Cell> existingKeys;
    /**
     * Some conditional updates may partially succeed; if so, {@link Cell}s which were known to be successfully
     * committed may be placed in this list. This list may not be complete; there may be additional cells that
     * were actually successfully committed but are not in this list.
     */
    @SuppressWarnings("checkstyle:MutableException") // Not final for backwards compatibility in serialization.
    private ImmutableList<Cell> knownSuccessfullyCommittedKeys;

    public KeyAlreadyExistsException(String msg, Throwable ex) {
        this(msg, ex, ImmutableList.of());
    }

    public KeyAlreadyExistsException(String msg) {
        this(msg, ImmutableList.of());
    }

    public KeyAlreadyExistsException(String msg, Throwable ex, Iterable<Cell> existingKeys) {
        super(msg, ex);
        this.existingKeys = ImmutableList.copyOf(existingKeys);
        knownSuccessfullyCommittedKeys = ImmutableList.of();
    }

    public KeyAlreadyExistsException(String msg, Iterable<Cell> existingKeys) {
        this(msg, existingKeys, ImmutableList.of());
    }

    public KeyAlreadyExistsException(String msg,
            Iterable<Cell> existingKeys,
            Iterable<Cell> knownSuccessfullyCommittedKeys) {
        super(msg);
        this.existingKeys = ImmutableList.copyOf(existingKeys);
        this.knownSuccessfullyCommittedKeys = ImmutableList.copyOf(knownSuccessfullyCommittedKeys);
    }

    public Collection<Cell> getExistingKeys() {
        return existingKeys;
    }

    public Collection<Cell> getKnownSuccessfullyCommittedKeys() {
        return knownSuccessfullyCommittedKeys;
    }

    private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
        stream.defaultReadObject();
        knownSuccessfullyCommittedKeys = MoreObjects.firstNonNull(knownSuccessfullyCommittedKeys, ImmutableList.of());
    }
}
