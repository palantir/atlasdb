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

    private static final Supplier<String> ec2SnitchSupplier = () -> "org.apache.cassandra.locator.Ec2Snitch";
    private static final Supplier<HostLocation> ec2LocationSupplier = () -> HostLocation.of("dc2", "rack2");

    @Test
    public void shouldReturnOverrideLocation() {
        Optional<HostLocation> overrideLocation = Optional.of(HostLocation.of("dc1", "rack1"));

        Supplier<Optional<HostLocation>> hostLocationSupplier = new HostLocationSupplier(ec2SnitchSupplier,
                ec2LocationSupplier, overrideLocation);

        assertThat(hostLocationSupplier.get()).isEqualTo(overrideLocation);
    }

    @Test
    public void shouldReturnEc2Location() {
        Supplier<Optional<HostLocation>> hostLocationSupplier = new HostLocationSupplier(ec2SnitchSupplier,
                ec2LocationSupplier, Optional.empty());

        assertThat(hostLocationSupplier.get()).isPresent();
        assertThat(hostLocationSupplier.get().get()).isEqualTo(ec2LocationSupplier.get());
    }

    @Test
    public void shouldReturnEmptyLocationFromUnexpectedSnitch() {
        Supplier<String> unexpectedSnitchSupplier = () -> "unexpected snitch";

        Supplier<Optional<HostLocation>> hostLocationSupplier = new HostLocationSupplier(unexpectedSnitchSupplier,
                ec2LocationSupplier, Optional.empty());

        assertThat(hostLocationSupplier.get()).isNotPresent();
    }

    @Test
    public void shouldReturnEmptyLocationFromSnitchException() {
        Supplier<String> badSnitchSupplier = () -> {
            throw new RuntimeException();
        };

        Supplier<Optional<HostLocation>> hostLocationSupplier = new HostLocationSupplier(badSnitchSupplier,
                ec2LocationSupplier, Optional.empty());

        assertThat(hostLocationSupplier.get()).isNotPresent();
    }

    @Test
    public void shouldReturnEmptyLocationFromEc2Exception() {
        Supplier<HostLocation> ec2BadLocationSupplier = () -> {
            throw new RuntimeException();
        };

        Supplier<Optional<HostLocation>> hostLocationSupplier = new HostLocationSupplier(ec2SnitchSupplier,
                ec2BadLocationSupplier, Optional.empty());

        assertThat(hostLocationSupplier.get()).isNotPresent();
    }

    @Test
    public void shouldReturnHostLocationInCassandraStyle() {
        HostLocation awsLocation = HostLocation.of("us-east", "1a");
        assertThat(Ec2HostLocationSupplier.parseHostLocation("us-east-1a")).isEqualTo(awsLocation);
    }

}
