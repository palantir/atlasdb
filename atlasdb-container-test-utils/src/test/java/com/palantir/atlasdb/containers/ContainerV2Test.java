/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.containers;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class ContainerV2Test {
    public static List<ContainerV2> containers() {
        return List.of(new CassandraContainerV2(), new ThreeNodeCassandraCluster());
    }

    @ParameterizedTest
    @MethodSource("containers")
    public void dockerComposeFileShouldExist(ContainerV2 container) {
        assertThat(ContainerV2Test.class.getResource(container.getDockerComposeFile()))
                .isNotNull();
    }
}