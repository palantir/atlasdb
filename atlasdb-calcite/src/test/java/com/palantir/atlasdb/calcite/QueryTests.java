/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.calcite;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Callable;

public final class QueryTests {
    private QueryTests() {
        // uninstantiable
    }

    public static int count(ResultSet results) throws SQLException {
        int i = 0;
        while (results.next()) {
            i++;
        }
        return i;
    }

    public static void assertFails(Callable<?> c) {
        try {
            c.call();
        } catch (Exception e) {
            return; // success
        }
        throw new RuntimeException("the call did not fail");
    }
}
