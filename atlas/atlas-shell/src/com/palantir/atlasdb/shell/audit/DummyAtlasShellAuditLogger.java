package com.palantir.atlasdb.shell.audit;



/**
 * This is a dummy implementation of the AtlasShellAuditLogger,
 * used in instances of AtlasShell which do not need audit logging.
 * Swallows the events whole, so we don't have to worry about them.
 *
 * @author dxiao
 */
public class DummyAtlasShellAuditLogger implements AtlasShellAuditLogger {

    @Override
    public void userExecutedScriptlet(long sessionId, String scriptlet) {
        // *gulp*
    }

    @Override
    public void logError(long sessionId, String message) {
        // *gulp*
    }

    @Override
    public void logOutput(long sessionId, String message) {
        // *gulp*
    }

    @Override
    public void logInput(long sessionId, String message) {
        // *gulp*
    }

}
