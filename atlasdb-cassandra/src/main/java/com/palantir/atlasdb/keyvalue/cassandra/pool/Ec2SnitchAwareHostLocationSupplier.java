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

import java.util.Optional;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.palantir.logsafe.SafeArg;

public class Ec2SnitchAwareHostLocationSupplier implements HostLocationSupplier {
    private static final Logger log = LoggerFactory.getLogger(Ec2SnitchAwareHostLocationSupplier.class);

    private final Supplier<String> snitchSupplier;
    private final Supplier<HostLocation> ec2Supplier;

    public static HostLocationSupplier create(Supplier<String> snitchSupplier) {
        return new Ec2SnitchAwareHostLocationSupplier(snitchSupplier, new Ec2HostLocationSupplier());
    }

    @VisibleForTesting
    Ec2SnitchAwareHostLocationSupplier(Supplier<String> snitchSupplier, Supplier<HostLocation> ec2Supplier) {
        this.snitchSupplier = Suppliers.memoize(snitchSupplier::get);
        this.ec2Supplier = Suppliers.memoize(ec2Supplier::get);
    }

    @Override
    public Optional<HostLocation> get() {
        try {
            String snitch = snitchSupplier.get();
            log.debug("Snitch successfully detected", SafeArg.of("snitch", snitch));

            if ("org.apache.cassandra.locator.Ec2Snitch".equals(snitch)) {
                return Optional.of(ec2Supplier.get());
            }
            return Optional.empty();
        } catch (RuntimeException e) {
            log.warn("Host location supplier failed to retrieve the host location", e);
            return Optional.empty();
        }
    }
}
