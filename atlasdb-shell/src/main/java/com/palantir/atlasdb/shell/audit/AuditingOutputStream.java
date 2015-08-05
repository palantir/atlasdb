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
