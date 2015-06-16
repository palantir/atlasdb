// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.palantir.atlasdb.cleaner.AsyncPuncherTest;
import com.palantir.atlasdb.cleaner.InMemorySweeperTest;
import com.palantir.atlasdb.cleaner.PuncherTest;
import com.palantir.atlasdb.compress.CompressionUtilsTest;
import com.palantir.atlasdb.keyvalue.impl.RangeRequestsTest;

@RunWith(Suite.class)
@SuiteClasses({
                CompressionUtilsTest.class,
                PuncherTest.class,
                InMemorySweeperTest.class,
                AsyncPuncherTest.class,
                RangeRequestsTest.class,
    })
public class AllAtlasDbFastTests {
}
