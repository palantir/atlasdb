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

public final class HostLocationSupplier implements Supplier<Optional<HostLocation>> {

    private final String snitch;
    private Optional<HostLocation> defaultLocation;


    public HostLocationSupplier(String snitch, Optional<HostLocation> defaultLocation) {
        this.snitch = snitch;
        this.defaultLocation = defaultLocation;
    }

    public HostLocationSupplier(String snitch) {
        this(snitch, Optional.empty());
    }

    @Override
    public Optional<HostLocation> get() {

        switch (snitch) {
            case "org.apache.cassandra.locator.EC2Snitch":
                return new EC2HostLocationSupplier().get();
            default:
                return defaultLocation;
        }
    }
}
