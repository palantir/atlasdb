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
package com.palantir.atlasdb.keyvalue.dbkvs;

public final class OracleErrorConstants {
    private OracleErrorConstants() {
        // utility class
    }

    public static final String ORACLE_CONSTRAINT_VIOLATION_ERROR = "ORA-00001";
    public static final String ORACLE_ALREADY_EXISTS_ERROR = "ORA-00955";
    public static final String ORACLE_NOT_EXISTS_ERROR = "ORA-00942";
    public static final String ORACLE_DUPLICATE_COLUMN_ERROR = "ORA-00957";
}
