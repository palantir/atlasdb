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
package com.palantir.atlasdb;

import com.palantir.atlasdb.cleaner.AsyncPuncherTest;
import com.palantir.atlasdb.cleaner.InMemorySweepTaskRunnerTest;
import com.palantir.atlasdb.cleaner.PuncherTest;
import com.palantir.atlasdb.compress.CompressionUtilsTest;
import com.palantir.atlasdb.keyvalue.impl.RangeRequestsTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
                CompressionUtilsTest.class,
                PuncherTest.class,
                InMemorySweepTaskRunnerTest.class,
                AsyncPuncherTest.class,
                RangeRequestsTest.class
        })
public class AllAtlasDbFastTests {
}
