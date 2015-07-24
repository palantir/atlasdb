package com.palantir.atlasdb.shell;

import com.palantir.common.exception.PalantirRuntimeException;

public class AtlasShellInterruptException extends PalantirRuntimeException {

    private static final long serialVersionUID = 1L;

    public AtlasShellInterruptException() {
        /**/
    }

    public AtlasShellInterruptException(Throwable n) {
        super(n);
    }

    public AtlasShellInterruptException(String msg) {
        super(msg);
    }

    public AtlasShellInterruptException(String msg, Throwable n) {
        super(msg, n);
    }

}
