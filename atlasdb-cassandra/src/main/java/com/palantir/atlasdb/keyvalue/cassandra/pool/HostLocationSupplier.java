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
import java.util.Optional;
import java.util.function.Supplier;

final class HostLocationSupplier implements Supplier<Optional<HostLocation>> {
    private static final int NUM_RETRIES = 5;

    @VisibleForTesting
    static final String EC2_SNITCH = "org.apache.cassandra.locator.Ec2Snitch";

    private final Supplier<Optional<String>> snitchSupplier;
    private final Supplier<Optional<HostLocation>> ec2Supplier;
    private final Optional<HostLocation> overrideLocation;

    @VisibleForTesting
    HostLocationSupplier(
            Supplier<Optional<String>> snitchSupplier,
            Supplier<Optional<HostLocation>> ec2Supplier,
            Optional<HostLocation> overrideLocation) {
        this.snitchSupplier = Suppliers.memoize(snitchSupplier::get);
        this.ec2Supplier = Suppliers.memoize(ec2Supplier::get);
        this.overrideLocation = overrideLocation;
    }

    static HostLocationSupplier create(Supplier<String> snitchSupplier, Optional<HostLocation> overrideLocation) {
        return new HostLocationSupplier(
                ExceptionHandlingSupplier.create(snitchSupplier, NUM_RETRIES),
                ExceptionHandlingSupplier.create(new Ec2HostLocationSupplier(), NUM_RETRIES),
                overrideLocation);
    }

    @Override
    public Optional<HostLocation> get() {
        if (overrideLocation.isPresent()) {
            return overrideLocation;
        }

        Optional<String> maybeSnitch = snitchSupplier.get();

        return maybeSnitch.flatMap(snitch -> {
            switch (snitch) {
                case EC2_SNITCH:
                    return ec2Supplier.get();
                default:
                    return Optional.empty();
            }
        });
    }
}
