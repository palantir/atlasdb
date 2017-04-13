/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.lock.logger.security;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by davidt on 4/12/17.
 */
public class StringEncoderTest {
    private static final String testString = "Test String";

    @Test
    public void encryptTest() throws Exception {
        StringEncoder se = new StringEncoder();
        Assert.assertEquals(se.decrypt(se.encrypt(testString)), testString);
    }
}