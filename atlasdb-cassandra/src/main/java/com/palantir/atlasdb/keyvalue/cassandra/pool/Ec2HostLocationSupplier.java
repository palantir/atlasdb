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

package com.palantir.atlasdb.keyvalue.cassandra.pool;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.CharStreams;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.function.Supplier;

/**
 * Returns the client's datacenter and rack derived from Cassandra's Ec2Snitch.
 * <p>
 * AWS has an endpoint that returns the datacenter and rack (in Cassandra terms) - this request will fail if not on AWS.
 * The reply comes in the form "datacenter"+"rack", e.g. "us-east-1a", where datacenter is "us-east-1" and rack is "a".
 */
public final class Ec2HostLocationSupplier implements Supplier<HostLocation> {
    private static final SafeLogger log = SafeLoggerFactory.get(Ec2HostLocationSupplier.class);
    private static final String AZ_URL = "http://169.254.169.254/latest/meta-data/placement/availability-zone";

    @Override
    public HostLocation get() {
        try {
            URL url = new URL(AZ_URL);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            int responseCode = conn.getResponseCode();
            Preconditions.checkState(responseCode == 200, "Getting AWS host metadata was not successful");
            try (InputStreamReader reader = new InputStreamReader(conn.getInputStream())) {
                return parseHostLocation(CharStreams.toString(reader));
            }
        } catch (IOException e) {
            log.warn(
                    "Could not query AWS host metadata to retrieve the host location. "
                            + "We are either not running on AWS or don't have access to the AWS host metadata service",
                    e);
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    static HostLocation parseHostLocation(String responseBody) {
        // The result of this parsing must match Cassandra's as closely as possible, as the output is later matched.

        // Split strings such as "us-east-1a" into "us-east" and "1a"
        String[] splitResponse = responseBody.split("-");
        String rack = splitResponse[splitResponse.length - 1];

        // this hack accounts for certain Cassandra cases
        String datacenter = responseBody.substring(0, responseBody.length() - 1);
        if (datacenter.endsWith("1")) {
            datacenter = responseBody.substring(0, responseBody.length() - 3);
        }

        return HostLocation.of(datacenter, rack);
    }
}
