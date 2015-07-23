package com.palantir.atlasdb.shell.audit;

import com.palantir.common.annotation.NonIdempotent;

public interface AtlasShellAuditLogger {

    @NonIdempotent
    void userExecutedScriptlet(long sessionId, String scriptlet);

    @NonIdempotent
    void logError(long sessionId, String message);

    @NonIdempotent
    void logOutput(long sessionId, String message);

    @NonIdempotent
    void logInput(long sessionId, String message);
}
