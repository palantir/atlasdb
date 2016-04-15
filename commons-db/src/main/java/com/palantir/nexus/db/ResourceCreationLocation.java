package com.palantir.nexus.db;

import java.io.Serializable;

import org.joda.time.format.ISODateTimeFormat;

import com.google.common.base.MoreObjects;

/**
 * A RuntimeException that represents the creation location of a resource (e.g. JDBC Connection or
 * ResultSet) to help identify potential resource leaks. This instance tracks the stack trace of
 * resource creation as well as the thread name and ID of the creator.
 */
public class ResourceCreationLocation extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final ThreadInfo creatingThreadInfo;

    public ResourceCreationLocation() {
        this("This is where the DB connection was created"); //$NON-NLS-1$
    }

    public ResourceCreationLocation(String message) {
        this(message, null);
    }

    public ResourceCreationLocation(String message, Exception cause) {
        super(message, cause);
        this.creatingThreadInfo = ThreadInfo.from(Thread.currentThread());
    }

    public ThreadInfo getCreatingThreadInfo() {
        return creatingThreadInfo;
    }

    @Override
    public String getMessage() {
        return super.getMessage() + " by " + creatingThreadInfo;
    }

    /**
     * Used to avoid strong references to {@link Thread}
     */
    public static class ThreadInfo implements Serializable {
        private static final long serialVersionUID = 1L;

        private final String name;
        private final long id;
        private final long timestamp;

        public static ThreadInfo from(Thread t) {
            return new ThreadInfo(t.getName(), t.getId());
        }

        private ThreadInfo(String name, long id) {
            super();
            this.name = name;
            this.id = id;
            this.timestamp = System.currentTimeMillis();
        }

        public String getName() {
            return name;
        }

        public long getId() {
            return id;
        }

        public long getTimestamp() {
            return timestamp;
        }

        @Override
       public String toString() {
            return MoreObjects.toStringHelper("thread")
                    .add("name", name)
                    .add("ID", id)
                    .add("timestamp", ISODateTimeFormat.dateTime().print(timestamp))
                    .toString();
        }
    }

}
