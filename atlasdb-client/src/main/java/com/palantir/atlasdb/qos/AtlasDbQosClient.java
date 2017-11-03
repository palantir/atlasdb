/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.qos;

// TODO this class should use a QosService and update limits
public class AtlasDbQosClient {

    // The KVS layer should call this before every read/write operation
    public void checkLimit() {
        // always return immediately - i.e. no backoff
        // TODO this call should "use up" a unit
        // TODO if soft-limited, pause
        // TODO if hard-limited, throw exception
    }
}
