package com.palantir.nexus.db;

import org.apache.commons.lang.Validate;

public class SqlClause {

    private final String key;
    private final String clause;

    public SqlClause(String key, String clause) {
        Validate.notNull(key);
        Validate.notNull(clause);
        this.key = key;
        this.clause = clause;
    }

    public String getClause() {
        return clause;
    }

    public String getKey() {
        return key;
    }

    @Override
    public String toString() {
        return key + ": " + clause; //$NON-NLS-1$
    }
}
