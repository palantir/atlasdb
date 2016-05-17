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
package com.palantir.sql;

import java.sql.Blob;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.sql.BasicSQL;

public class Blobs {
    private final static Logger sqlExceptionlog = Logger.getLogger("sqlException." + Blobs.class.getName());

    public static long length(Blob b) throws PalantirSqlException {
        try {
            return b.length();
        } catch (SQLException e) {
            sqlExceptionlog.info("Caught SQLException", e);
            throw BasicSQL.handleInterruptions(0, e);
        }
    }

    public static byte[] getBytes(Blob b, long pos, int length) throws PalantirSqlException {
        try {
            return b.getBytes(pos, length);
        } catch (SQLException e) {
            sqlExceptionlog.info("Caught SQLException", e);
            throw BasicSQL.handleInterruptions(0, e);
        }
    }
}
