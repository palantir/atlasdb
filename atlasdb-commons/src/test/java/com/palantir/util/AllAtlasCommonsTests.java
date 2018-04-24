/*
 * (c) Copyright 2015 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.util;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.palantir.common.base.ThrowablesTest;
import com.palantir.util.crypto.Sha256HashTest;
import com.palantir.util.paging.PageDrainerTest;

@RunWith(Suite.class)
@SuiteClasses({
    ThrowablesTest.class,
    Sha256HashTest.class,
    PageDrainerTest.class,
})
public class AllAtlasCommonsTests {
    // blank
}
