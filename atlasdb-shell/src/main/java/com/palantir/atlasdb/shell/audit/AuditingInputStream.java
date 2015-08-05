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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.google.common.base.Preconditions;

/**
 * Wraps an {@link InputStream} so that any input read is also forwarded to an audit logger.
 *
 * Note that this class only relays bytes to another {@link OutputStream},
 * and does not perform any buffering or multi-byte character defragmentation.
 *
 * @author dxiao
 */
public class AuditingInputStream extends FilterInputStream {

    private final OutputStream auditLogger;

    public AuditingInputStream (InputStream stream, OutputStream auditLogger) {
        super(stream);
        Preconditions.checkNotNull(auditLogger, "Must have a valid Audit Logging Session for input stream.");
        this.auditLogger = auditLogger;
    }

    @Override
    public int read() throws IOException {
        char b = (char)in.read();
        auditLogger.write(b);
        return b;
    }

    /*
     * Note that read(byte[] b) is covered by this method as well, as discussed in
     * {@link java.io.FilderInputStream#read(byte[])}
     */
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int result = in.read(b, off, len);
        auditLogger.write(b, off, result);
        return result;
    }
}
