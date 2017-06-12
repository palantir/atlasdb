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
package com.palantir.exception;

import java.sql.SQLException;
import java.util.Optional;

import com.palantir.common.exception.PalantirRuntimeException;

/**
 * SQLExceptions are checked. However, generally speaking, we just want to propagate them.
 * Having a whole bunch of 'throws' and 'catch throws' is ugly & unnecessary.
 *
 */
public class PalantirSqlException extends PalantirRuntimeException {
    private static final long serialVersionUID = 1L;
    public static enum DO_NOT_SET_INITIAL_SQL_EXCEPTION { YES};
    public static enum SET_INITIAL_SQL_EXCEPTION {YES};

    /**
     * @deprecated Do not use! This should only be used by Throwables.rewrap which
     * constructs new exceptions via reflection and relies on constructors with
     * particular signatures being present.
     */
    @Deprecated
    public PalantirSqlException(String message, Throwable t) {
        super(message, t);
    }

    protected PalantirSqlException(DO_NOT_SET_INITIAL_SQL_EXCEPTION i) {
        super();
    }

    protected PalantirSqlException(DO_NOT_SET_INITIAL_SQL_EXCEPTION i, String msg) {
        super(msg);
    }

    /**
     * This is not safe to use with Throwables.chain()
     */
    protected PalantirSqlException(SET_INITIAL_SQL_EXCEPTION i) {
        super(new SQLException());
    }

    protected PalantirSqlException(SET_INITIAL_SQL_EXCEPTION i, String msg) {
        super(msg, new SQLException(msg));
    }

    protected PalantirSqlException(String msg, SQLException n) {
        super(msg, n);
    }

    public static PalantirSqlException create() {
        return new PalantirSqlException(SET_INITIAL_SQL_EXCEPTION.YES);
    }

    public static PalantirSqlException create(String msg) {
        return new PalantirSqlException(SET_INITIAL_SQL_EXCEPTION.YES, msg);
    }

    public static PalantirSqlException create(SQLException e) {
        String msg = Optional.ofNullable(e.getMessage()).orElse(e.getClass().getName() + "with null message");
        return new PalantirSqlException(msg, e);
    }

    public static PalantirSqlException createForChaining() {
        return new PalantirSqlException(DO_NOT_SET_INITIAL_SQL_EXCEPTION.YES);
    }

    public static PalantirSqlException createForChaining(String msg) {
        return new PalantirSqlException(DO_NOT_SET_INITIAL_SQL_EXCEPTION.YES, msg);
    }
}

