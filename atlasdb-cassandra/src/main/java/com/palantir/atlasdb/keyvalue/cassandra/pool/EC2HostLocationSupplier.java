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

import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.util.Optional;
import java.util.function.Supplier;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public final class EC2HostLocationSupplier implements Supplier<Optional<HostLocation>> {

    @Override
    public Optional<HostLocation> get() {
        OkHttpClient client = new OkHttpClient.Builder()
                .build();
        try {
            Response response = client.newCall(new Request.Builder()
                    .get()
                    .url("http://169.254.169.254/latest/meta-data/placement/availability-zone")
                    .build())
                    .execute();
            checkState(response.isSuccessful(), "Getting AWS host metadata was not successful");

            String responseString = response.body().string();
            String datacentre = responseString.substring(0, responseString.length()-2);
            String rack = responseString.substring(responseString.length()-1);

            return Optional.of(ImmutableHostLocation.builder().datacentre(datacentre).rack(rack).build());
        } catch (IOException e) {
            throw new RuntimeException("Could not communicate with AWS host metadata", e);
        }
    }
}
