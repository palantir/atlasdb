/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.nexus.streaming;

import com.palantir.common.io.ForwardingInputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.security.DigestInputStream;
import java.security.MessageDigest;

public class PTInputStream extends ForwardingInputStream implements Serializable {
    private static final long serialVersionUID = 1L;

    private final InputStream is;
    private final long length;

    public PTInputStream(InputStream is, long length) {
        this.is = is;
        this.length = length;
    }

    public PTInputStream withDigestInputStreamDecorator(MessageDigest digest) {
        return new PTInputStream(new DigestInputStream(is, digest), length);
    }

    @Override
    protected InputStream delegate() {
        return is;
    }

    public long getLength() {
        return length;
    }

    @Override
    public String toString() {
        return "PTInputStream [length=" + length + ", inputStream=" + is + "]"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
}
