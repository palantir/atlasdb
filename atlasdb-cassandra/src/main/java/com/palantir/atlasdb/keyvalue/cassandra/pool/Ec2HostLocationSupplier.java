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
import com.google.common.base.Suppliers;
import com.palantir.logsafe.Preconditions;
import java.io.IOException;
import java.util.function.Supplier;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Returns the client's datacenter and rack derived from Cassandra's Ec2Snitch.
 * <p>
 * AWS has an endpoint that returns the datacenter and rack (in Cassandra terms) - this request will fail if not on AWS.
 * The reply comes in the form "datacenter"+"rack", e.g. "us-east-1a", where datacenter is "us-east-1" and rack is "a".
 */
public final class Ec2HostLocationSupplier implements Supplier<HostLocation> {
    private static final Logger log = LoggerFactory.getLogger(Ec2HostLocationSupplier.class);

    /*
       This is a supplier to avoid class loading races breaking downstream internal products.
    */
    private static final Supplier<OkHttpClient> client = Suppliers.memoize(() -> new OkHttpClient.Builder().build());

    @Override
    public HostLocation get() {
        try {
            Response response = client.get()
                    .newCall(new Request.Builder()
                            .get()
                            .url("http://169.254.169.254/latest/meta-data/placement/availability-zone")
                            .build())
                    .execute();
            Preconditions.checkState(response.isSuccessful(), "Getting AWS host metadata was not successful");

            return parseHostLocation(response.body().string());
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
