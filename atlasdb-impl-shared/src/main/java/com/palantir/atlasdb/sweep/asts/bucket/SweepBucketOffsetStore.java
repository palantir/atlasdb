/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.asts.bucket;

/**
 * Stores information about bucket offsets.
 */
public interface SweepBucketOffsetStore {
    // WRITE OFFSETS
    // when writing a new bucket, we GET the current write offset
    // AND THEN attempt to write at the current write offset
    // AND THEN, if successful, update the write offset.
    long getWriteOffset();

    void setWriteOffset();

    // READ OFFSETS
    // the bucket retriever reads the read offset
    // the background task, when making progress, is allowed to CAS the read offset forward, if fully processed.
}
