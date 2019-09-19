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
package com.palantir.nexus.db;

import com.google.common.base.MoreObjects;
import java.io.Serializable;
import org.joda.time.format.ISODateTimeFormat;

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
