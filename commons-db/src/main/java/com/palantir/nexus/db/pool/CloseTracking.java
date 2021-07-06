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
package com.palantir.nexus.db.pool;

import com.google.common.base.FinalizableReferenceQueue;
import com.google.common.base.FinalizableWeakReference;
import com.google.common.collect.Sets;
import com.palantir.nexus.db.ResourceCreationLocation;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CloseTracking {
    private static final Logger log = LoggerFactory.getLogger(CloseTracking.class);

    private CloseTracking() {
        // nope
    }

    public static ConnectionManager wrap(ConnectionManager delegate) {
        return wrap(ResourceTypes.CONNECTION_MANAGER, delegate);
    }

    public static Connection wrap(Connection delegate) {
        return wrap(ResourceTypes.CONNECTION, delegate);
    }

    public static <R, E extends Exception> R wrap(final ResourceType<R, E> type, final R delegate) {
        if (!log.isErrorEnabled()) {
            // We leave this check in in case this goes haywire in the field.
            // This way setting the log level for CloseTracking to OFF can
            // still disable it entirely.
            return delegate;
        }
        final Tracking tracking = new Tracking(type.name());
        R wrapped = type.closeWrapper(delegate, () -> {
            tracking.close();
            type.close(delegate);
        });
        destructorReferences.add(new MyReference(wrapped, tracking));
        return wrapped;
    }

    private static final class Tracking {
        private final String typeName;
        private final Throwable createTrace;

        private boolean closed = false;

        public Tracking(String typeName) {
            this.typeName = typeName;
            this.createTrace = new ResourceCreationLocation("This is where the " + typeName + " was allocated");
        }

        public synchronized void close() {
            closed = true;
        }

        @SuppressWarnings("BadAssert") // only fail close check with asserts enabled
        public synchronized void check() {
            if (!closed) {
                log.error("{} never closed!", typeName, createTrace);
                assert false : typeName + " never closed!" + "\n" + Arrays.toString(createTrace.getStackTrace());
            }
        }
    }

    private static final class MyReference<T> extends FinalizableWeakReference<T> {
        private final Tracking tracking;

        public MyReference(T referent, Tracking tracking) {
            super(referent, destructorQueue);

            this.tracking = tracking;
        }

        @Override
        public void finalizeReferent() {
            try {
                tracking.check();
            } finally {
                destructorReferences.remove(this);
            }
        }
    }

    // We maintain hard references to the custom weak references since
    // otherwise they themselves can get collected and thus never enqueued.
    private static final Set<MyReference<?>> destructorReferences =
            Sets.newSetFromMap(new ConcurrentHashMap<MyReference<?>, Boolean>());
    private static final FinalizableReferenceQueue destructorQueue = new FinalizableReferenceQueue();
}
