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
package com.palantir.atlasdb.keyvalue.dbkvs;

public final class OracleErrorConstants {

    private OracleErrorConstants() {
        //utility class
    }

    public static final String ORACLE_ALREADY_EXISTS_ERROR = "ORA-00955";
    public static final String ORACLE_NOT_EXISTS_ERROR = "ORA-00942";
    public static final String ORACLE_UNIQUE_CONSTRAINT_ERROR =  "ORA-00001";
}
