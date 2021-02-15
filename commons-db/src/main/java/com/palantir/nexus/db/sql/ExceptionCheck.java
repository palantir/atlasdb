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

/**
 * Helper methods for checking if Throwables contain various SQL errors.
 * @author dstipp
 *
 */
public final class ExceptionCheck {
    // TODO: we should probably try to figure out how to wire in DBType here?

    private ExceptionCheck() {
        //
    }

    /**
     * Check if throwable contains message about unique constraint violation.
     * @param e
     * @return true if unique constraint violation
     */
    public static boolean isUniqueConstraintViolation(Throwable e) {
        if (throwableContainsMessage(e, "ORA-00001") // Oracle "ORA-00001: unique constraint <constraint name> violated"
                || throwableContainsMessage(
                        e,
                        "duplicate key value violates unique constraint") // PostgreSQL "ERROR: duplicate key violates
                // unique constraint"
                || throwableContainsMessage(
                        e,
                        "Unique index or primary key violation") // H2 "SEVERE: Unique index or primary key violation"
                || throwableContainsMessage(e, "SQLITE_CONSTRAINT") // SQLite
        ) {
            return true;
        }
        return false;
    }

    /**
     * Check if throwable contains message about foreign key constraint violation.
     * @param e
     * @return true if foreign key constraint violation
     */
    public static boolean isForeignKeyConstraintViolation(Throwable e) {
        if (throwableContainsMessage(
                        e,
                        "ORA-02291") // Oracle "ORA-02291: integrity constraint <constraint name> violated - parent key
                // not found"
                || throwableContainsMessage(
                        e, "ORA-02292") // Oracle "ORA-02292: integrity constraint <constraint name> violated - child
                // record found"
                || throwableContainsMessage(
                        e,
                        "violates foreign key constraint") // PostgreSQL "ERROR:  insert or update on table "<constraint
                // name>" violates foreign key constraint "<constraint name>
                // ""
                || throwableContainsMessage(
                        e, "Referential integrity constraint violation") // H2 "SEVERE: Referential integrity constraint
                // violation"
                || throwableContainsMessage(e, "SQLITE_CONSTRAINT") // SQLite
        ) {
            return true;
        }
        return false;
    }

    /**
     * Check if throwable contains message about invalid time zone.
     * @param e
     * @return true if timezone invalid
     */
    public static boolean isTimezoneInvalid(Throwable e) {
        if (throwableContainsMessage(e, "ORA-01882") // Oracle "ORA-01882: timezone region not found"
        // Doesn't seem to matter anywhere else right now.
        ) {
            return true;
        }
        return false;
    }

    private static boolean throwableContainsMessage(Throwable e, String... messages) {
        for (Throwable ex = e; ex != null; ex = ex.getCause()) {
            for (String message : messages) {
                if (String.valueOf(ex.getMessage()).contains(message)) {
                    return true;
                }
            }
            if (ex == ex.getCause()) {
                return false;
            }
        }
        return false;
    }
}
