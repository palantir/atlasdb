/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.api.expectations;

import org.immutables.value.Value;

/**
 *  Info about one specific {@link com.palantir.atlasdb.keyvalue.api.KeyValueService} read/get method call.
 */
@Value.Immutable
public interface KvsCallReadInfo extends Comparable<KvsCallReadInfo> {
    long bytesRead();

    String kvsMethodName();

    @Override
    default int compareTo(KvsCallReadInfo other) {
        return Long.compare(this.bytesRead(), other.bytesRead());
    }
}
