package com.palantir.atlasdb.shell.audit;


/**
 * A connection to an {@link AtlasShellAuditLogger}, remote or local.
 * AtlasShell uses this connection to create a unique {@link AuditLoggingSession} and sessionId
 * per executed script and/or database connection.
 *
 * There should be only one AuditLoggingConnection instance per endpoint.
 *
 * @author dxiao
 */
public class AuditLoggingConnection {
    private final AtlasShellAuditLogger auditLogger;

    /**
     * Create a dummy audit logging connection.
     *
     * Used when audit logging is disabled.
     */
    public static AuditLoggingConnection loggingDisabledCreateDummyConnection() {
        return new AuditLoggingConnection(new DummyAtlasShellAuditLogger());
    }

    /**
     * Wrap an existing {@link AtlasShellAuditLogger} interface in an AuditLoggingConnection.
     */
    public AuditLoggingConnection(AtlasShellAuditLogger auditLogger) {
        this.auditLogger = auditLogger;
    }

    /**
     * Create a new {@link AuditLoggingSession} with a random sessionId.
     */
    public AuditLoggingSession getNewSession() {
        return new AuditLoggingSession(auditLogger);
    }
}
