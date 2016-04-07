package com.palantir.nexus.db.monitoring.timer;

import com.palantir.nexus.db.sql.BasicSQLString;

/**
 * Code extracted from {@link BasicSQLString}. This provides one of several competing and
 * overlapping mechanisms to register monitoring callbacks for various SQL actions.
 *
 * Class for tracking timing information for a single SQL call. An instance of {@link Handle} should
 * be constructed before making the call, and {@link Handle#stop()} should be called after the call
 * completes.
 */
public interface SqlTimer {
    Handle start(String module, String sqlKey, String rawSql);

    interface Handle {
        /**
         * Called when the SQL call is complete. Updates the SQL statistics and writes timing
         * information to the timing logger.
         */
        void stop();
    }
}