/**
 * Copyright 2015 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.shell.audit;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;

import com.google.common.annotations.VisibleForTesting;


/**
 * Endpoint for logging one user's/scriptlet's AtlasShell activities using an {@link AtlasShellAuditLogger}.
 *
 * Events logged using an instance of AuditLoggingSession will show the same sessionId.
 * AuditLoggingSessions should therefore be instantiated with unique id's.
 *
 * This class also provides some convenience methods for interacting with
 * {@link AuditingInputStream} and {@link AuditingOutputStream}, in the form of {@link StreamLogger}.
 *
 * @author dxiao
 */
public class AuditLoggingSession {

    private static final int BUFFER_FLUSH_THRESHOLD = 10*1024;

    private final AtlasShellAuditLogger auditLogger;
    private final long sessionId;
    private StreamLogger inputStreamLogger;
    private StreamLogger outputStreamLogger;
    private StreamLogger errorStreamLogger;

    /**
     * Create a new AuditLoggingSession with a random (probably unique) sessionId.
     */
    public AuditLoggingSession (AtlasShellAuditLogger auditLogger) {
        this(auditLogger, new Random().nextLong());
    }

    @VisibleForTesting
    public AuditLoggingSession (AtlasShellAuditLogger auditLogger, long sessionId) {
        this.auditLogger = auditLogger;
        this.sessionId = sessionId;
        instantiateStreamLoggers();
    }

    public void userExecutedScriptlet(String scriptlet) {
        auditLogger.userExecutedScriptlet(sessionId, scriptlet);
    }

    public void logError(String message) {
        auditLogger.logError(sessionId, message);
    }

    public void logOutput(String message) {
        auditLogger.logOutput(sessionId, message);
    }

    public void logInput(String message) {
        auditLogger.logInput(sessionId, message);
    }

    @VisibleForTesting
    long getSessionId() {
        return sessionId;
    }

    private void instantiateStreamLoggers() {
        inputStreamLogger = new StreamLogger() {
            @Override
            protected void logMessage(String message) {
                AuditLoggingSession.this.logInput(message);
            }
        };
        outputStreamLogger = new StreamLogger() {
            @Override
            protected void logMessage(String message) {
                AuditLoggingSession.this.logOutput(message);
            }
        };
        errorStreamLogger = new StreamLogger() {
            @Override
            protected void logMessage(String message) {
                AuditLoggingSession.this.logError(message);
            }
        };
    }

    public StreamLogger getInputLogger() {
        return inputStreamLogger;
    }

    public StreamLogger getOutputLogger() {
        return outputStreamLogger;
    }

    public StreamLogger getErrorLogger() {
        return errorStreamLogger;
    }

    /**
     * Flushes all {@link StreamLogger}s instantiated on this AuditLoggingSession,
     * creating audit log events for any streams with buffered content.
     *
     * Only call this method when sure that the any pending write()'s to the stream loggers
     * have completed.
     * This prevents fragmented messages or messages with corrupted multi-byte characters.
     */
    public void flushStreamLoggers() {
        if (inputStreamLogger != null) {
            inputStreamLogger.flush();
        }
        if (outputStreamLogger != null) {
            outputStreamLogger.flush();
        }
        if (errorStreamLogger != null) {
            errorStreamLogger.flush();
        }
    }

    /**
     * Buffers messages forwarded to this session from an {@link InputStream} or {@link OutputStream}.
     *
     * The buffer can be flushed and thus a logging event created with the buffered contents
     * either through {@link #flush()} or {@link AuditLoggingSession#flushStreamLoggers()}.
     * The buffer is also flushed when the buffer exceeds 10KB bytes and ends in a new line.
     *
     * @see AuditingInputStream
     * @see AuditingOutputStream
     *
     * @author dxiao
     */
    private abstract class StreamLogger extends OutputStream {
        private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        @Override
        public void write(int b) {
            buffer.write(b);
            if (b == '\n') {
                flushIfBufferBig();
            }
        }

        @Override
        public void write(byte[] b, int off, int len) {
            buffer.write(b, off, len);
            if (b[off+len-1] == '\n') {
                flushIfBufferBig();
            }
        }

        private void flushIfBufferBig() {
            if (buffer.size() > BUFFER_FLUSH_THRESHOLD) {
                logMessage(buffer.toString());
                buffer.reset();
            }
        }

        /**
         * Only call this method when sure that the any pending write()'s to the stream loggers
         * have completed.
         * This prevents fragmented messages or messages with corrupted multi-byte characters.
         */
        @Override
        public void flush () {
            if (buffer.size() > 0) {
                logMessage(buffer.toString());
                buffer.reset();
            }
        }

        /**
         * Implementations of this method should forward the message passed
         * to the appropriate AuditLoggingSession method.
         */
        protected abstract void logMessage (String message);
    }
}
