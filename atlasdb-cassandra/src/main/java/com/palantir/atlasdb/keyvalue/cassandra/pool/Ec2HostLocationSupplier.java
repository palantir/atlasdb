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
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.logsafe.Preconditions;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

/**
 * Returns the client's datacenter and rack derived from Cassandra's Ec2Snitch.
 * <p>
 * AWS has an endpoint that returns the datacenter and rack (in Cassandra terms) - this request will fail if not on AWS.
 * The reply comes in the form "datacenter"+"rack", e.g. "us-east-1a", where datacenter is "us-east-1" and rack is "a".
 */
public final class Ec2HostLocationSupplier implements Supplier<HostLocation> {

    private static final OkHttpClient client = new OkHttpClient.Builder().build();

    @Override
    public HostLocation get() {
        try {
            Response response = client.newCall(new Request.Builder()
                    .get()
                    .url("http://169.254.169.254/latest/meta-data/placement/availability-zone")
                    .build())
                    .execute();
            Preconditions.checkState(response.isSuccessful(),
                    "Getting AWS host metadata was not successful");

            return parseHostLocation(response.body().string());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    static HostLocation parseHostLocation(String az) {
        // Code is copied from Cassandra's Ec2Snitch. The result of this parsing must match Cassandra's as closely as
        // possible, as the strings are later matched exactly.

        String ec2region;
        String ec2zone;

        // Split "us-east-1a" or "asia-1a" into "us-east"/"1a" and "asia"/"1a".
        String[] splits = az.split("-");
        ec2zone = splits[splits.length - 1];

        // hack for CASSANDRA-4026
        ec2region = az.substring(0, az.length() - 1);
        if (ec2region.endsWith("1")) {
            ec2region = az.substring(0, az.length() - 3);
        }

        return HostLocation.of(ec2region, ec2zone);


    }
}
