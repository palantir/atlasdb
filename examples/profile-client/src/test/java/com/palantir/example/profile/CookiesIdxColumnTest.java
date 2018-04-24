/*
 * (c) Copyright 2016 Palantir Technologies Inc. All rights reserved.
 * ​
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * ​
 * http://opensource.org/licenses/BSD-3-Clause
 * ​
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.example.profile;

import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import com.palantir.example.profile.schema.generated.UserProfileTable.CookiesIdxTable.CookiesIdxColumn;

public class CookiesIdxColumnTest {
    @Test
    public void testHashCode() {
        UUID uuid = new UUID(3, 4);
        CookiesIdxColumn column1 = CookiesIdxColumn.of(new byte[]{1}, new byte[]{2, 4}, uuid);
        CookiesIdxColumn column2 = CookiesIdxColumn.of(new byte[]{1}, new byte[]{2, 4}, uuid);
        Assert.assertEquals(column1.hashCode(), column2.hashCode());
    }

    @Test
    public void testHashCodeUnequal() {
        UUID uuid = new UUID(5, 6);
        CookiesIdxColumn column1 = CookiesIdxColumn.of(new byte[]{1, 3}, new byte[]{2, 4}, uuid);
        CookiesIdxColumn column2 = CookiesIdxColumn.of(new byte[]{1, 2}, new byte[]{2, 4}, uuid);
        Assert.assertNotEquals(column1.hashCode(), column2.hashCode());
    }
}
