package com.palantir.nexus.db.sql.monitoring.logger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlLoggers {
    public static final Logger LOGGER = LoggerFactory.getLogger("com.palantir.nexus.db.SQL");
    public static final Logger CANCEL_LOGGER = LoggerFactory.getLogger("com.palantir.nexus.db.SQL.cancel");
    public static final Logger SQL_EXCEPTION_LOG = LoggerFactory.getLogger("sqlException.com.palantir.nexus.db.sql.BasicSQL");
}
