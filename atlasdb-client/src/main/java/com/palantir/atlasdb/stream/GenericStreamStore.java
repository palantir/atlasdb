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
package com.palantir.atlasdb.stream;

import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.util.crypto.Sha256Hash;
import java.io.File;
import java.io.InputStream;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Interface for storing streams specifically for atlasdb.
 */
public interface GenericStreamStore<ID> {
    int BLOCK_SIZE_IN_BYTES = 1000000; // 1MB. DO NOT CHANGE THIS WITHOUT AN UPGRADE TASK

    /**
     * Implemented in the generated StreamStore implementation.
     * Use this to look up stream IDs, given a hash of the corresponding InputStream's content.
     */
    Map<Sha256Hash, ID> lookupStreamIdsByHash(Transaction tx, final Set<Sha256Hash> hashes);

    /**
     * @deprecated use #loadSingleStream instead.
     *
     * Returns an InputStream if such a stream exists, throwing an exception if no stream exists.
     */
    @Deprecated
    InputStream loadStream(Transaction tx, ID id);

    /**
     * Loads the stream with ID id, returning {@code Optional.empty} if no such stream exists.
     */
    Optional<InputStream> loadSingleStream(Transaction tx, ID id);

    /**
     * Loads the streams for each ID in ids.
     * If an id has no corresponding stream, it will be omitted from the returned map.
     */
    Map<ID, InputStream> loadStreams(Transaction tx, Set<ID> ids);

    /**
     * Loads the whole stream, and saves it to a local temporary file.
     */
    File loadStreamAsFile(Transaction tx, ID id);
}
