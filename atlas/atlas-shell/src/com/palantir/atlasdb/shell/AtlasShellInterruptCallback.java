package com.palantir.atlasdb.shell;

import java.util.Set;

import com.google.common.collect.Sets;

public class AtlasShellInterruptCallback {

    private final Set<AtlasShellInterruptListener> interruptListeners = Sets.newHashSet();

    public void interrupt() {
        synchronized(interruptListeners) {
            for (AtlasShellInterruptListener interruptListener : interruptListeners) {
                interruptListener.interrupt();
            }
        }
    }

    public void registerInterruptListener(AtlasShellInterruptListener interruptListener) {
        synchronized(interruptListeners) {
            interruptListeners.add(interruptListener);
        }
    }

    public void unregisterInterruptListener(AtlasShellInterruptListener interruptListener) {
        synchronized(interruptListeners) {
            interruptListeners.remove(interruptListener);
        }
    }
}
