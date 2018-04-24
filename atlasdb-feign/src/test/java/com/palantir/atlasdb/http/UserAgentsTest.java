/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.http;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.BlockingDeque;

import org.hamcrest.MatcherAssert;
import org.junit.Test;

public class UserAgentsTest {
    private static final String PACKAGE_VERSION = "1.2";
    private static final String PACKAGE_TITLE = "package";
    private static final String PACKAGE_USER_AGENT
            = String.format(UserAgents.USER_AGENT_FORMAT, PACKAGE_TITLE, PACKAGE_VERSION);
    private static final String DEFAULT_USER_AGENT
            = String.format(UserAgents.USER_AGENT_FORMAT, UserAgents.DEFAULT_VALUE, UserAgents.DEFAULT_VALUE);

    @Test
    public void userAgentIncludesAtlasDb() {
        MatcherAssert.assertThat(UserAgents.fromStrings(PACKAGE_TITLE, PACKAGE_VERSION), is(PACKAGE_USER_AGENT));
    }

    @Test
    public void canGetUserAgentDataFromPackage() {
        Package classPackage = mock(Package.class);
        when(classPackage.getImplementationVersion()).thenReturn(PACKAGE_VERSION);
        when(classPackage.getImplementationTitle()).thenReturn(PACKAGE_TITLE);

        MatcherAssert.assertThat(UserAgents.fromPackage(classPackage), is(PACKAGE_USER_AGENT));
    }

    @Test
    public void addsDefaultUserAgentDataIfUnknown() {
        Package classPackage = mock(Package.class);
        when(classPackage.getImplementationTitle()).thenReturn(null);
        when(classPackage.getImplementationVersion()).thenReturn(null);

        MatcherAssert.assertThat(UserAgents.fromPackage(classPackage), is(DEFAULT_USER_AGENT));
    }

    @Test
    public void canGetUserAgentDataFromClass() {
        Class<BlockingDeque> clazz = BlockingDeque.class;

        String expectedUserAgent = UserAgents.fromStrings(
                clazz.getPackage().getImplementationTitle(),
                clazz.getPackage().getImplementationVersion());
        MatcherAssert.assertThat(UserAgents.fromClass(clazz), is(expectedUserAgent));
    }
}
