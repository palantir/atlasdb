package com.palantir.nexus.streaming;

import java.io.InputStream;
import java.io.Serializable;
import java.security.DigestInputStream;
import java.security.MessageDigest;

import com.palantir.common.io.ForwardingInputStream;

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
