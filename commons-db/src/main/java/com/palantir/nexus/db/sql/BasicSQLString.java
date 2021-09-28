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
package com.palantir.nexus.db.sql;

public class BasicSQLString {

    @Override
    public String toString() {
        return "SQLString [key=" + key + ", sql=" + sql + "]"; // $NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    private final String key;
    private final String sql;

    public BasicSQLString(String key, String sql) {
        this.key = key;
        this.sql = sql;
    }

    public String getQuery() {
        return sql;
    }

    public String getKey() {
        return key;
    }

    @Override
    public int hashCode() {
        final int PRIME = 31;
        int result = 1;
        result = PRIME * result + ((sql == null) ? 0 : sql.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final BasicSQLString other = (BasicSQLString) obj;
        if (sql == null) {
            if (other.sql != null) {
                return false;
            }
        } else if (!sql.equals(other.sql)) {
            return false;
        }
        return true;
    }

    /**
     * This class is a SQLString that is only created when it has the correct sqlquery for a given key
     * unregistered query string.  In other words, if there has been a rewrite for a given query, the
     * FinalSQLString has that rewrite.  Objects of this type should only be created after making such checks,
     * which is why the constructor is private to SQLString.
     * @author dcohen
     *
     */
    public static class FinalSQLString {

        @Override
        public String toString() {
            return "FinalSQLString [delegate=" + delegate + "]"; // $NON-NLS-1$ //$NON-NLS-2$
        }

        protected final BasicSQLString delegate;

        /**
         * Should only be called inside SQLString because this class essentially verifies that we've
         * checked for updates.
         */
        public FinalSQLString(BasicSQLString sqlstring) {
            this.delegate = sqlstring;
        }

        public FinalSQLString(String key, String sql) {
            this(new BasicSQLString(key, sql));
        }

        String getQuery() {
            return delegate.getQuery();
        }

        /**
         * This will be null if this is not a registered query
         */
        public String getKey() {
            return delegate.getKey();
        }
    }
}
