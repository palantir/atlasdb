/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

import org.junit.Test;

public class Ec2HostLocationSupplierTest {

    @Test
    public void parseHostLocation() {
        HostLocation hostLocation = Ec2HostLocationSupplier.parseHostLocation("us-east-1a");
        assertThat(hostLocation).isNotNull();
        assertThat(hostLocation.datacenter()).isEqualTo("us-east");
        assertThat(hostLocation.rack()).isEqualTo("1a");
        assertThat(hostLocation.isProbablySameRackAs("us-east", "1a")).isTrue();
        assertThat(hostLocation.isProbablySameRackAs("us-east", "1b")).isFalse();
        assertThat(hostLocation.isProbablySameRackAs("us-east", "2a")).isFalse();
        assertThat(hostLocation.isProbablySameRackAs("us-west", "1a")).isFalse();
    }
}
