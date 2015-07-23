package com.palantir.atlasdb.shell;

import java.util.concurrent.atomic.AtomicBoolean;

public class AtlasShellInterruptListener {

    private final AtomicBoolean interrupted = new AtomicBoolean(false);

    public void interrupt() {
        interrupted.set(true);
    }

    public boolean isInterrupted() {
        return interrupted.get();
    }
}
