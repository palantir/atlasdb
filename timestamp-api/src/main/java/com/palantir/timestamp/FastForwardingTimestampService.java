/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.timestamp;

public interface FastForwardingTimestampService extends TimestampService {
    /**
     * Fast forwards the timestamp to the specified one so that no one can be served fresh timestamps prior
     * to it from now on.
     *
     * The caller of this is responsible for not using any of the fresh timestamps previously served to it,
     * and must call getFreshTimestamps() to ensure it is using timestamps after the fastforward point.
     *
     * @param newMinimumTimestamp
     */
    void fastForwardTimestamp(long newMinimumTimestamp);
}
