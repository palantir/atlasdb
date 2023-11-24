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

import java.util.Arrays;
import java.util.Collection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/* TODO(boyoruk): Delete when JUnit5 upgrade is over. */
@RunWith(Parameterized.class)
public class ContainerTest {
    @Parameterized.Parameters(name = "With container {0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {{new CassandraContainer()}});
    }

    private final Container container;

    public ContainerTest(Container container) {
        this.container = container;
    }

    @Test
    public void dockerComposeFileShouldExist() {
        assertThat(ContainerTest.class.getResource(container.getDockerComposeFile()))
                .isNotNull();
    }
}
