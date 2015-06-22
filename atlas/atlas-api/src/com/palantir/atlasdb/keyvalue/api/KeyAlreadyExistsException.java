// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.keyvalue.api;

import java.util.Collection;

import com.google.common.collect.ImmutableList;
import com.palantir.common.exception.PalantirRuntimeException;

public class KeyAlreadyExistsException extends PalantirRuntimeException {
    private static final long serialVersionUID = 1L;

    private final ImmutableList<Cell> existingKeys;

    public KeyAlreadyExistsException(String msg, Throwable n) {
        this(msg, n, ImmutableList.<Cell>of());
    }

    public KeyAlreadyExistsException(String msg) {
        this(msg, ImmutableList.<Cell>of());
    }

    public KeyAlreadyExistsException(String msg, Throwable n, Iterable<Cell> keys) {
        super(msg, n);
        existingKeys = ImmutableList.copyOf(keys);
    }

    public KeyAlreadyExistsException(String msg, Iterable<Cell> keys) {
        super(msg);
        existingKeys = ImmutableList.copyOf(keys);
    }

    public Collection<Cell> getExistingKeys() {
        return existingKeys;
    }
}
