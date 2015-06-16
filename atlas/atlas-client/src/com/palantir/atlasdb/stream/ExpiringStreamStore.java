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

package com.palantir.atlasdb.stream;

import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.palantir.util.crypto.Sha256Hash;

/**
 * Interface for storing streams specifically for atlas.
 */
public interface ExpiringStreamStore<T> extends GenericStreamStore<T> {
    Sha256Hash storeStream(T id, InputStream stream, long duration, TimeUnit durationUnit);
    Map<T, Sha256Hash> storeStreams(Map<T, InputStream> streams, long duration, TimeUnit durationUnit);
}
