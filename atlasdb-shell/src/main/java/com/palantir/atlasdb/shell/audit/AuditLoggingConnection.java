/**
 * // Copyright 2015 Palantir Technologies
 * //
 * // Licensed under the BSD-3 License (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * // http://opensource.org/licenses/BSD-3-Clause
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */
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
