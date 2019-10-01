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

import java.io.IOException;
import java.util.Optional;
import java.util.function.Supplier;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

/**
 * Returns the client's datacentre and rack derived from Cassandra's EC2Snitch.
 * <p>
 * AWS has an endpoint that returns the datacentre and rack (in Cassandra terms) - this request will fail if not on AWS.
 * The reply comes in the form "datacentre"+"rack", e.g. "us-east-1a", where datacentre is "us-east-1" and rack is "a".
 */
public final class EC2HostLocationSupplier implements Supplier<Optional<HostLocation>> {

    OkHttpClient client = new OkHttpClient.Builder().build();

    @Override
    public Optional<HostLocation> get() {
        try {
            Response response = client.newCall(new Request.Builder()
                    .get()
                    .url("http://169.254.169.254/latest/meta-data/placement/availability-zone")
                    .build())
                    .execute();
            com.palantir.logsafe.Preconditions.checkState(response.isSuccessful(),
                    "Getting AWS host metadata was not successful");

            String responseString = response.body().string();
            String datacentre = responseString.substring(0, responseString.length() - 2);
            String rack = responseString.substring(responseString.length() - 1);

            return Optional.of(HostLocation.of(datacentre, rack));
        } catch (IOException e) {
            return Optional.empty();
        }
    }
}
