package com.palantir.nexus.db.pool;

public abstract class ResourceSharer<R, E extends Exception> {
    private final ResourceType<R, E> type;

    private R delegate = null;
    private int refs = 0;

    public ResourceSharer(ResourceType<R, E> type) {
        this.type = type;
    }

    private synchronized void deref() throws E {
        --refs;
        if (refs > 0) {
            return;
        }
        R delegateLocal = delegate;
        delegate = null;

        // close under lock in case underlying is crappy enough to not
        // handle multiple concurrent co-existing.
        type.close(delegateLocal);
    }

    public synchronized R get() {
        if (delegate == null) {
            delegate = open();
        }
        ++refs;
        final R delegateLocal = delegate;
        return type.closeWrapper(delegate, new ResourceOnClose<E>() {
            private boolean closed = false;

            @Override
            public synchronized void close() throws E {
                if (closed) {
                    // arggh
                    return;
                }
                closed = true;
                deref();
            }
        });
    }

    protected abstract R open();
}
