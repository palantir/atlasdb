/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.logger;

public class SqlStackLogWrapper {
    private String query;
    private String key;
    private boolean isRegistered;

    public SqlStackLogWrapper(String query) {
        this.query = query;
        isRegistered = false;
    }

    public SqlStackLogWrapper(String query, String key) {
        this.query = query;
        this.key = key;
        isRegistered = true;
    }

    public String getQuery() {
        return query;
    }

    public String getKey() {
        return key;
    }

    public boolean isRegistered() {
        return isRegistered;
    }

    @Override
    public String toString() {
        return query.toString();
    }
}
