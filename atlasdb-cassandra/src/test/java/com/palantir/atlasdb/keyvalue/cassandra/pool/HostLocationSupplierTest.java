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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import java.util.function.Supplier;
import org.junit.Test;

public class HostLocationSupplierTest {
    private static final Supplier<Optional<String>> EC2_SNITCH_SUPPLIER =
            () -> Optional.of(HostLocationSupplier.EC2_SNITCH);
    private static final Supplier<Optional<HostLocation>> EC2_LOCATION_SUPPLIER =
            () -> Optional.of(HostLocation.of("dc2", "rack2"));

    @Test
    public void shouldReturnOverrideLocation() {
        Optional<HostLocation> overrideLocation = Optional.of(HostLocation.of("dc1", "rack1"));

        Supplier<Optional<HostLocation>> hostLocationSupplier =
                new HostLocationSupplier(EC2_SNITCH_SUPPLIER, EC2_LOCATION_SUPPLIER, overrideLocation);

        assertThat(hostLocationSupplier.get()).isEqualTo(overrideLocation);
    }

    @Test
    public void shouldReturnEc2Location() {
        Supplier<Optional<HostLocation>> hostLocationSupplier =
                new HostLocationSupplier(EC2_SNITCH_SUPPLIER, EC2_LOCATION_SUPPLIER, Optional.empty());

        assertThat(hostLocationSupplier.get()).isEqualTo(EC2_LOCATION_SUPPLIER.get());
    }

    @Test
    public void shouldReturnEmptyLocationFromUnexpectedSnitch() {
        Supplier<Optional<String>> unexpectedSnitchSupplier = () -> Optional.of("unexpected snitch");

        Supplier<Optional<HostLocation>> hostLocationSupplier =
                new HostLocationSupplier(unexpectedSnitchSupplier, EC2_LOCATION_SUPPLIER, Optional.empty());

        assertThat(hostLocationSupplier.get()).isEmpty();
    }

    @Test
    public void shouldReturnEmptyLocationFromEmptySnitch() {
        Supplier<Optional<String>> failedSnitchSupplier = Optional::empty;

        Supplier<Optional<HostLocation>> hostLocationSupplier =
                new HostLocationSupplier(failedSnitchSupplier, EC2_LOCATION_SUPPLIER, Optional.empty());

        assertThat(hostLocationSupplier.get()).isEmpty();
    }

    @Test
    public void shouldReturnEmptyLocationFromEC2LocationSupplierException() {
        Supplier<HostLocation> badEc2Supplier = () -> {
            throw new RuntimeException();
        };

        Supplier<Optional<HostLocation>> hostLocationSupplier = new HostLocationSupplier(
                EC2_SNITCH_SUPPLIER, ExceptionHandlingSupplier.create(badEc2Supplier, 5), Optional.empty());

        assertThat(hostLocationSupplier.get()).isEmpty();
    }

    @Test
    public void shouldReturnEmptyLocationFromSnitchException() {
        Supplier<String> badSnitchSupplier = () -> {
            throw new RuntimeException();
        };

        Supplier<Optional<HostLocation>> hostLocationSupplier = new HostLocationSupplier(
                ExceptionHandlingSupplier.create(badSnitchSupplier, 5), EC2_LOCATION_SUPPLIER, Optional.empty());

        assertThat(hostLocationSupplier.get()).isEmpty();
    }

    @Test
    public void shouldReturnEmptyLocationWhenEverythingFails() {
        Supplier<String> badSnitchSupplier = () -> {
            throw new RuntimeException();
        };
        Supplier<HostLocation> badEc2Supplier = () -> {
            throw new RuntimeException();
        };

        Supplier<Optional<HostLocation>> hostLocationSupplier = new HostLocationSupplier(
                ExceptionHandlingSupplier.create(badSnitchSupplier, 5),
                ExceptionHandlingSupplier.create(badEc2Supplier, 5),
                Optional.empty());

        assertThat(hostLocationSupplier.get()).isEmpty();
    }

    @Test
    public void shouldReturnHostLocationInCassandraStyle() {
        HostLocation awsLocation = HostLocation.of("us-east", "1a");
        assertThat(Ec2HostLocationSupplier.parseHostLocation("us-east-1a")).isEqualTo(awsLocation);
    }
}
