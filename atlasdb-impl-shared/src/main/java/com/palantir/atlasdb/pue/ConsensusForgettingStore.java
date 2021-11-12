/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.pue;

import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import java.util.Map;
import java.util.Optional;

public interface ConsensusForgettingStore {
    void putUnlessExists(Cell cell, byte[] value) throws KeyAlreadyExistsException;

    void putUnlessExists(Map<Cell, byte[]> values) throws KeyAlreadyExistsException;

    void checkAndTouch(Cell cell, byte[] value) throws CheckAndSetException;

    ListenableFuture<Optional<byte[]>> get(Cell cell);

    void put(Cell cell, byte[] value);

    void put(Map<Cell, byte[]> values);
}
