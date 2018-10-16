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
@SuppressWarnings("checkstyle:AbbreviationAsWordInName") // Don't wish to break the API.
public class VerboseSQLException extends SQLException {
    private static final long serialVersionUID = 1L;

    // Transient so detailed debugging information is not shipped to the client.
    private final transient String verboseErrorMessage;

    public VerboseSQLException(SQLException exception, String verboseErrorMessage) {
        super(exception.getMessage(), exception.getSQLState(), exception.getErrorCode());
        setNextException(exception.getNextException());
        setStackTrace(exception.getStackTrace());
        if (exception.getCause() != null) {
            initCause(exception.getCause());
        } else if (exception.getNextException() != null) {
            initCause(exception.getNextException());
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
