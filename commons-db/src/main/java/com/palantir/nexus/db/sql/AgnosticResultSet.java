package com.palantir.nexus.db.sql;


import java.util.List;

public interface AgnosticResultSet {

    public List<? extends AgnosticResultRow> rows();

    public AgnosticResultRow get(int row);

    public int size();

}
