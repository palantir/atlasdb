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
package com.palantir.atlasdb.timelock.copycat;

import javax.ws.rs.QueryParam;

import com.palantir.timestamp.TimestampRange;
import com.palantir.timestamp.TimestampService;

import io.atomix.copycat.client.CopycatClient;

public class CopycatTimestampService implements TimestampService {
    private final CopycatClient copycatClient;
    private final String namespace;

    public CopycatTimestampService(CopycatClient copycatClient, String namespace) {
        this.copycatClient = copycatClient;
        this.namespace = namespace;
    }

    @Override
    public long getFreshTimestamp() {
        return getFreshTimestamps(1).getLowerBound();
    }

    @Override
    public TimestampRange getFreshTimestamps(@QueryParam("number") int numTimestampsRequested) {
        return copycatClient.submit(new FreshTimestampsCommand(namespace, numTimestampsRequested)).join();
    }
}
