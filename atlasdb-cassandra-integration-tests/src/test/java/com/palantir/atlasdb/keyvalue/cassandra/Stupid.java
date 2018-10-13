/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import com.palantir.flake.FlakeRetryingRule;
import com.palantir.flake.ShouldRetry;

@ShouldRetry
public class Stupid {
    public static int test = 0;

    @Rule
    public RuleChain chain = RuleChain.outerRule(new FlakeRetryingRule());

    @Before
    public void setup() {
        Awaitility.await().atMost(10, TimeUnit.MINUTES).until(() -> {
            Thread.sleep(500);
            test = 0;
            return true;
        });
    }

    @Test
    public void test() {
        for (int i = 0; i < 100; i++) {
            test++;
        }
        throw new RuntimeException();
    }
}
