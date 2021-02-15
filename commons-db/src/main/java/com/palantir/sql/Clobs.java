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
package com.palantir.sql;

import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.sql.BasicSQL;
import java.sql.Clob;
import java.sql.SQLException;

public class Clobs {

    public static String getString(Clob clob, long clobLen) throws PalantirSqlException {
        try {
            if (clob == null || clobLen == 0L) {
                return ""; //$NON-NLS-1$
            }

            StringBuilder rv = new StringBuilder();
            long pos = 1;
            if (clobLen < 0) {
                // then we have to ask Oracle
                clobLen = clob.length();
            }

            while (pos <= clobLen) {
                int readLen = 32000;
                if ((pos + readLen - 1) > clobLen) {
                    readLen = (int) (clobLen - pos + 1);
                }

                rv.append(clob.getSubString(pos, readLen));

                pos += readLen;
            }

            return rv.toString();
        } catch (SQLException e) {
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static String getString(Clob clob, int clobLength) throws PalantirSqlException {
        return getString(clob, (long) clobLength);
    }
}
