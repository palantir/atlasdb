/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.util.sql;

import java.sql.SQLException;

/**
 * Extends {@link SQLException} to include an optional verbose error message
 * which may contain useful debugging information (such as the values of any
 * arguments passed to the database).
 *
 * <p>Because the verbose error message may potentially contain sensitive
 * information, the error message is <i>not</i> serialized when transported from
 * the server to the client.
 */
public class VerboseSQLException extends SQLException {
    private static final long serialVersionUID = 1L;

    // Transient so detailed debugging information is not shipped to the client.
    private transient String verboseErrorMessage;

    public VerboseSQLException(SQLException e, String verboseErrorMessage) {
        super(e.getMessage(), e.getSQLState(), e.getErrorCode());
        setNextException(e.getNextException());
        setStackTrace(e.getStackTrace());
        if (e.getCause() != null) {
            initCause(e.getCause());
        } else if (e.getNextException() != null){
            initCause(e.getNextException());
        }
        this.verboseErrorMessage = verboseErrorMessage;
    }

    @Override
    public String getMessage() {
        if (null == verboseErrorMessage) {
            return super.getMessage();
        }
        StringBuilder sb = new StringBuilder();
        sb.append(verboseErrorMessage);
        sb.append(": ");
        sb.append(super.getMessage());
        return sb.toString();
    }
}
