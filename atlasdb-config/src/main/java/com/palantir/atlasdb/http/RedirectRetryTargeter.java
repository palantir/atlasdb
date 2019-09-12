/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.http;

import java.net.URL;
import java.util.List;

import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;

public class RedirectRetryTargeter {
    private final URL nextServerBaseUrl;

    private RedirectRetryTargeter(URL nextServerBaseUrl) {
        this.nextServerBaseUrl = nextServerBaseUrl;
    }

    public static RedirectRetryTargeter create(URL localServer, List<URL> clusterUrls) {
        int localServerIndex = clusterUrls.indexOf(localServer);
        Preconditions.checkArgument(localServerIndex != -1,
                "Local server not found in the list of cluster URLs.",
                SafeArg.of("localServer", localServer),
                SafeArg.of("clusterUrls", clusterUrls));

        int nextServerIndex = (localServerIndex + 1) % clusterUrls.size();
        return new RedirectRetryTargeter(clusterUrls.get(nextServerIndex));
    }

    URL redirectRequest() {
        return nextServerBaseUrl;
    }
}
