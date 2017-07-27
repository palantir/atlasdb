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

package com.palantir.atlasdb.timelock.clock;

public class RequestTime {
    // Since we expect localTimeAtStart != localTimeAtEnd, it's safe to have an empty request time.
    public static final RequestTime EMPTY = new RequestTime(0L, 0L, 0L);

    public final long localTimeAtStart;
    public final long localTimeAtEnd;
    public final long remoteSystemTime;

    RequestTime(long localTimeAtStart, long localTimeAtEnd, long remoteSystemTime) {
        this.localTimeAtStart = localTimeAtStart;
        this.localTimeAtEnd = localTimeAtEnd;
        this.remoteSystemTime = remoteSystemTime;
    }
}
