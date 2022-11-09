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
package com.palantir.atlasdb.keyvalue.dbkvs.impl;

public interface DbDdlTable {
    void create(byte[] tableMetadata);

    /**
     * See {@link #drop(CaseSensitivity)}. This method uses CASE_SENSITIVE deletion.
     */
    void drop();

    /**
     * Drops the table, and deletes the table from the mapping table (if present), and from the metadata table.
     * If referenceCaseSensitivity is CASE_INSENSITIVE, and another table is created with a table reference that is a
     * case insensitive match with this table, then the behaviour is undefined.
     */
    void drop(CaseSensitivity referenceCaseSensitivity);

    void truncate();

    void checkDatabaseVersion();

    void compactInternally(boolean inSafeHours);
}
