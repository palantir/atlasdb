package com.palantir.atlasdb.config;

public enum LockLeader {
    SOMEONE_ELSE_IS_THE_LOCK_LEADER, I_AM_THE_LOCK_LEADER;

    public static final LockLeader iAmTheLockLeader(boolean iAmTheLockLeader) {
        if(iAmTheLockLeader) {
            return I_AM_THE_LOCK_LEADER;
        } else {
            return SOMEONE_ELSE_IS_THE_LOCK_LEADER;
        }
    }
}
