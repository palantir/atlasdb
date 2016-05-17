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
package com.palantir.nexus.db.sql;

import org.apache.commons.lang.Validate;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

public final class PostgresBlobs {
    private PostgresBlobs() {
        //
    }

    public static String getLimitedBlob(String columnName, int index) {
        long limit = SQL.POSTGRES_BLOB_READ_LIMIT;
        long start = 1 + index * limit;
        return "substring(" + columnName + " from " + start + " for " + limit + ") as " + columnName;
    }

    public static int getMaximumBlobIndex(int length) {
        return (length - 1) / SQL.POSTGRES_BLOB_READ_LIMIT;
    }

    public static void assemble(byte[] result, byte[] sub, int index) {
        if (index < getMaximumBlobIndex(result.length)) {
            Validate.isTrue(sub.length == SQL.POSTGRES_BLOB_READ_LIMIT);
        } else {
            Validate.isTrue(sub.length - 1 == (result.length - 1) % SQL.POSTGRES_BLOB_READ_LIMIT);
        }
        System.arraycopy(sub, 0, result, index * SQL.POSTGRES_BLOB_READ_LIMIT, sub.length);
    }

    public static byte[] getWholeBlob(PalantirSqlConnection conn,
                                      long pk,
                                      int blobLen,
                                      byte[] firstPart,
                                      String columnName,
                                      Function<Integer, String> getQueryFun) {
        byte[] result = new byte[blobLen];
        assemble(result, firstPart, 0);

        for (int i = 1; i <= getMaximumBlobIndex(blobLen); i++) {
            String query = getQueryFun.apply(i);
            AgnosticResultSet rs = conn.selectResultSetUnregisteredQuery(query, pk);
            AgnosticResultRow row = Iterables.getOnlyElement(rs.rows());
            byte[] blob = row.getBlob(columnName);
            assemble(result, blob, i);
        }
        return result;
    }

}
