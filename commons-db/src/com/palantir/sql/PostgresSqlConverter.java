package com.palantir.sql;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Contains some utility methods for converting Oracle SQL to Postgres.
 */
public class PostgresSqlConverter {
    /**
     * Convert a buffer of generic Oracle SQL into equivalent SQL
     * for execution on Postgres. This does not translate PL/SQL
     * or other magic Oracle-isms like CONNECT BY, which is why
     * the input needs to be "generic Oracle" SQL.
     *
     * @param in generic Oracle SQL. May not be null.
     * @return SQL for running on Postgres.
     */
    public static StringBuffer toPostgres(CharSequence in) {
        StringBuffer out = postgres_regexp(in);
        StringBuffer plSQL = generatePostgresSequences(in);
        out.append(plSQL);
        return out;
    }

    public static StringBuffer toPostgresSeedData(CharSequence in) {
        StringBuffer out = postgres_regexpSeedData(in);
        StringBuffer plSQL = generatePostgresSequences(in);
        out.append(plSQL);
        return out;
    }

    private static StringBuffer postgres_regexpSeedData(CharSequence in) {
        StringBuffer out = replace(in, "START WITH", "START"); //$NON-NLS-1$ //$NON-NLS-2$
        out = replace(out, "([A-Za-z0-9_]+)\\.nextval", "nextval('$1')"); //$NON-NLS-1$ //$NON-NLS-2$
        out = replace(out, "sysdate", "current_timestamp"); //$NON-NLS-1$ //$NON-NLS-2$

        //concat is tricky b/c you have to get rid of the close paren, too
        out = replace(out, "CONCAT\\(([A-Za-z0-9_]+)(\\s*,\\s*)([^\\)]*)(\\))", "$1 || $3"); //$NON-NLS-1$ //$NON-NLS-2$
        return out;
    }

    /**
     * Helper routine which scans an input buffer for sequence creation
     * statements and outputs the plpgsql for selecting multiple values
     * from those statements.
     *
     * @param in generic Oracle SQL. May not be null.
     * @return plpgsql for selecting multiple sequence values. Not null.
     */
    private static StringBuffer generatePostgresSequences(CharSequence in) {
        int flags = Pattern.CASE_INSENSITIVE | Pattern.MULTILINE;
        Pattern pattern = Pattern.compile("CREATE\\s+SEQUENCE\\s+([A-Za-z0-9_]+).*", flags); //$NON-NLS-1$
        Matcher matcher = pattern.matcher(in);
        StringBuffer out = new StringBuffer();
        while (matcher.find()) {
            String sequence = matcher.group(1);
            String plSQL = ";\n" + generatePostgresSequence(sequence); //$NON-NLS-1$
            out.append(plSQL);
        }
        return out;
    }

    // NB: only have newline at the end b/c of the sql splitter
    private static final String POSTGRES_NEXTVAL_TEMPLATE =
            "-- nextvalue function for %1%s\n" + //$NON-NLS-1$
                    "CREATE OR REPLACE FUNCTION nextvals_%1$s(n INT) " + //$NON-NLS-1$
                    "RETURNS SETOF BIGINT AS $$ " + //$NON-NLS-1$
                    "BEGIN " + //$NON-NLS-1$
                    "\tFOR i in 1 .. n LOOP " + //$NON-NLS-1$
                    "\t\tRETURN NEXT nextval('%1$s'); " + //$NON-NLS-1$
                    "\tEND LOOP; " + //$NON-NLS-1$
                    "\tRETURN; " + //$NON-NLS-1$
                    "END; " + //$NON-NLS-1$
                    "$$ LANGUAGE plpgsql;\n"; //$NON-NLS-1$

    /**
     * Helper routine that takes the name of a sequence and
     * generates plpgsql statements for computing multiple next
     * values from the sequence for Postgres.
     *
     * @param name valid SQL sequence name. May not be null.
     * @return plpgsql for selecting multiple sequence values. Not null
     */
    private static String generatePostgresSequence(String name) {
        if (name == null) {
            throw new IllegalArgumentException("null sequence name"); //$NON-NLS-1$
        }

        // NB: this needs to be lower case because that is what the
        // DB abstraction for next values expects
        String sqlName = name.toLowerCase();
        String sql = String.format(POSTGRES_NEXTVAL_TEMPLATE, sqlName);
        return sql;
    }

    private static String POSTGRES_DATA_TYPE = "(CHARACTER VARYING|TEXT|BYTEA|BIGINT|REAL)"; //$NON-NLS-1$

    /**
     * Convert generic Oracle SQL to SQL suitable for running on Postgres.
     * This does not handle PL/SQL or the creation of new sequences.
     *
     * @param in generic Oracle SQL.
     * @return SQL suitable for Postgres
     */
    private static StringBuffer postgres_regexp(CharSequence in) {
        StringBuffer out = replace(in, "VARCHAR2", "CHARACTER VARYING"); //$NON-NLS-1$ //$NON-NLS-2$
        out = replace(out, "SELECT\\sUNIQUE\\s", "SELECT DISTINCT "); //$NON-NLS-1$ //$NON-NLS-2$
        out = replace(out, "([ \t\n\r,\\(\\)])CLOB($|[ \t\n\r,\\(\\);])", "$1TEXT$2"); //$NON-NLS-1$ //$NON-NLS-2$
        out = replace(out, "([ \t\n\r,\\(\\)])BLOB($|[ \t\n\r,\\(\\);])", "$1BYTEA$2"); //$NON-NLS-1$ //$NON-NLS-2$
        out = replace(out, "([ \t\n\r,\\(\\)])NUMBER(\\(\\d+,?\\d*\\))?($|[ \t\n\r,\\(\\);])", "$1BIGINT$3"); //$NON-NLS-1$ //$NON-NLS-2$
        out = replace(out, "FLOAT(\\(\\d+,?\\d*\\))?", "REAL"); //$NON-NLS-1$ //$NON-NLS-2$
        out = replace(out, "START WITH", "START"); //$NON-NLS-1$ //$NON-NLS-2$
        out = replace(out, "([A-Za-z0-9_]+)\\.nextval", "nextval('$1')"); //$NON-NLS-1$ //$NON-NLS-2$
        out = replace(out, "sysdate", "current_timestamp"); //$NON-NLS-1$ //$NON-NLS-2$
        out = replace(out, "RAW\\(\\d+\\)", "BYTEA"); //$NON-NLS-1$ //$NON-NLS-2$ // RAW\((\d+)\)
        out = replace(out, "\\sMODIFY\\s", " ALTER COLUMN "); //$NON-NLS-1$ //$NON-NLS-2$
        out = replace(out,
                "(\\sALTER\\s+COLUMN\\s+[A-Za-z0-9_]+)\\s+" +  //$NON-NLS-1$
                        POSTGRES_DATA_TYPE + "\\s", " $1 "); //$NON-NLS-1$ //$NON-NLS-2$
        out = replace(out, "(\\sALTER\\s+COLUMN\\s+[A-Za-z0-9_]+)\\s+DEFAULT\\s", " $1 SET DEFAULT "); //$NON-NLS-1$ //$NON-NLS-2$
        out = replace(out, "(\\sALTER\\s+COLUMN\\s+[A-Za-z0-9_]+)\\s+NOT\\s+NULL", " $1 SET NOT NULL"); //$NON-NLS-1$ //$NON-NLS-2$
        out = replace(out, "(\\sALTER\\s+COLUMN\\s+[A-Za-z0-9_]+)\\s+NULL", " $1 DROP NOT NULL"); //$NON-NLS-1$ //$NON-NLS-2$
        out = replace(out, "(ALTER\\s+TABLE\\s+[A-Za-z0-9_]+\\s+DROP\\s+CONSTRAINT\\s+[A-Za-z0-9_]+)\\s+DROP\\s+INDEX", "$1"); //$NON-NLS-1$ //$NON-NLS-2$
        out = replace(out, "(CREATE\\s+SEQUENCE\\s.*)\\scache\\s+\\d+", "$1 cache 1"); //$NON-NLS-1$ //$NON-NLS-2$
        out = replace(out, "(ALTER\\s+SEQUENCE\\s.*)\\scache\\s+\\d+", "$1 cache 1"); //$NON-NLS-1$ //$NON-NLS-2$

        //concat is tricky b/c you have to get rid of the close paren, too
        out = replace(out, "CONCAT\\(([A-Za-z0-9_]+)(\\s*,\\s*)([^\\)]*)(\\))", "$1 || $3"); //$NON-NLS-1$ //$NON-NLS-2$
        return out;
    }

    /**
     * Helper routine for doing regular expression search and replace
     * over a StringBuffer. Matches the supplied pattern over the input
     * and replaces it with the supplied string, producing a new
     * StringBuffer. The replacement string can refer to matched subgroups
     * in the pattern with $N, where N is between 1 and 9 inclusive.
     *
     * @param in      input StringBuffer to be searched over. Not modified. May
     *                not be null.
     * @param pattern regular expression pattern to match. May not be null.
     * @param replace the String to replace the pattern. May not be null.
     * @return a new StringBuffer representing the regular expression
     * search and replace applied over the input.
     * @throws IllegalArgumentException if any parameter is null.
     */
    private static StringBuffer replace(CharSequence in, String pattern, String replace) {
        if (in == null) {
            throw new IllegalArgumentException("null buffer"); //$NON-NLS-1$
        }
        if (pattern == null) {
            throw new IllegalArgumentException("null pattern"); //$NON-NLS-1$
        }
        if (replace == null) {
            throw new IllegalArgumentException("null replacement"); //$NON-NLS-1$
        }

        int flags = Pattern.CASE_INSENSITIVE | Pattern.MULTILINE;
        Pattern p = Pattern.compile(pattern, flags);
        Matcher matcher = p.matcher(in);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            matcher.appendReplacement(sb, replace);
        }
        matcher.appendTail(sb);
        return sb;
    }
}
