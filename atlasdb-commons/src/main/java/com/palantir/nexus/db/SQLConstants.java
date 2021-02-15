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
package com.palantir.nexus.db;

public class SQLConstants {

    /**
     * Oracle limit query syntax.
     *
     * Note - Oracle row nums are 1-based.
     */
    public static final String SQL_ORACLE_LIMIT_QUERY = "SELECT * FROM ( %s ) WHERE rownum < ?"; // $NON-NLS-1$
    /**
     * Oracle limit and offset query syntax (paging).
     *
     * Note - Oracle row nums are 1-based.
     */
    public static final String SQL_ORACLE_LIMIT_OFFSET_QUERY =
            "SELECT PT_USER_OUTER_QUERY____B.* FROM ( " //$NON-NLS-1$
                    + "	SELECT PT_USER_INNER_QUERY____A.*, rownum as inner_num FROM ( " //$NON-NLS-1$
                    + "		%s " //$NON-NLS-1$
                    + "	) PT_USER_INNER_QUERY____A WHERE rownum < ? " //$NON-NLS-1$
                    + ") PT_USER_OUTER_QUERY____B WHERE PT_USER_OUTER_QUERY____B.inner_num >= ? "; //$NON-NLS-1$
}
