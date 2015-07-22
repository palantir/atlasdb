package com.palantir.atlasdb.shell.audit;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import com.google.common.base.Preconditions;

/**
 * Wraps an {@link OutputStream} so that any input read is also forwarded to an audit logger.
 *
 * Note that this class only relays bytes to another {@link OutputStream},
 * and does not perform any buffering or multi-byte character defragmentation.
 *
 * @see AuditLoggingSession.StreamLogger
 *
 * @author dxiao
 */
public class AuditingOutputStream extends FilterOutputStream {

    private final OutputStream auditLogger;

    public AuditingOutputStream(OutputStream stream, OutputStream auditLogger) {
        super(stream);
        Preconditions.checkNotNull(auditLogger, "Must have a valid Audit Logging Session for output stream.");
        this.auditLogger = auditLogger;
    }

    @Override
    public void write(int b) throws IOException {
        auditLogger.write(b);
        out.write(b);
    }

    /*
     * Note that write(byte[] b) is covered by this method is well, as discussed in
     * {@link java.io.FilterOutputStream#write(byte[])}
     */
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        auditLogger.write(b, off, len);
        out.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        auditLogger.flush();
        out.flush();
    }
}
