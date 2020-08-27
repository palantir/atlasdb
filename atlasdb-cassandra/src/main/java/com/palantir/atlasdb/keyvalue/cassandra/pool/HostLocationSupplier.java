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

import com.google.common.base.Suppliers;
import com.palantir.logsafe.SafeArg;
import java.util.Optional;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class HostLocationSupplier implements Supplier<Optional<HostLocation>> {

    private final Supplier<String> snitchSupplier;
    private final Supplier<HostLocation> ec2Supplier;
    private final Optional<HostLocation> overrideLocation;

    private static final Logger log = LoggerFactory.getLogger(HostLocationSupplier.class);


    public HostLocationSupplier(Supplier<String> snitchSupplier,
            Supplier<HostLocation> ec2Supplier,
            Optional<HostLocation> overrideLocation) {
        this.snitchSupplier = Suppliers.memoize(snitchSupplier::get);
        this.ec2Supplier = Suppliers.memoize(ec2Supplier::get);
        this.overrideLocation = overrideLocation;
    }

    public HostLocationSupplier(Supplier<String> snitchSupplier,
            Optional<HostLocation> overrideLocation) {
        this(snitchSupplier, new Ec2HostLocationSupplier(), overrideLocation);
    }

    @Override
    public Optional<HostLocation> get() {
        try {
            if (overrideLocation.isPresent()) {
                return overrideLocation;
            }

            String snitch = snitchSupplier.get();
            log.debug("Snitch successfully detected", SafeArg.of("snitch", snitch));

            switch (snitch) {
                case "org.apache.cassandra.locator.Ec2Snitch":
                    return Optional.of(ec2Supplier.get());
                default:
                    return Optional.empty();
            }
        } catch (RuntimeException e) {
            log.warn("Host location supplier failed to retrieve the host location", e);
            return Optional.empty();
        }
    }
}
