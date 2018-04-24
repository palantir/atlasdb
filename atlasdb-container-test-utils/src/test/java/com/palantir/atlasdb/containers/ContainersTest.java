/*
 * (c) Copyright 2016 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.containers;

import org.junit.Test;

@SuppressWarnings("checkstyle:IllegalThrows")
public class ContainersTest {
    @Test
    public void containerStartsUpCorrectly() throws Throwable {
        Containers containers = new Containers(ContainersTest.class)
                .with(new FirstNginxContainer());

        setupContainers(containers);
    }

    @Test
    public void multipleContainersStartUpCorrectly() throws Throwable {
        Containers containers = new Containers(ContainersTest.class)
                .with(new FirstNginxContainer());
        setupContainers(containers);

        containers = containers.with(new SecondNginxContainer());
        setupContainers(containers);
    }

    private static void setupContainers(Containers containers) throws Throwable {
        containers.before();
    }
}
