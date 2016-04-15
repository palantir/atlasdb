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

        if(System.getProperty("user.name").contains("bamboo")) {
            printStackTrace(); // (authorized)
        }
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
