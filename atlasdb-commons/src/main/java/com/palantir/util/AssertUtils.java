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
package com.palantir.util;

import com.palantir.logsafe.Arg;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.logsafe.logger.SafeLogger;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("BadAssert") // explicitly using asserts for test-only
public class AssertUtils {

    /**
     * Previously, failed asserts logged to this log.
     * However, this led to the problem that the logger would always be com.palantir.util.AssertUtils,
     * which is extremely annoying to try to filter based on your logging properties.
     * (Should it go into the server error log, or maybe the upgrade log, or the import log file?
     * Can't tell, cause it's all AssertUtils!)
     * <p>
     * Until we get all downstream projects off of using defaultLog however,
     * this will stay, just deprecated.
     */
    @Deprecated
    private static final Logger log = LoggerFactory.getLogger(AssertUtils.class);

    public static <T> boolean nonNullItems(Collection<T> c) {
        for (T t : c) {
            if (t == null) {
                return false;
            }
        }

        return true;
    }

    public static <T> boolean assertListElementsUnique(List<T> l) {
        Set<T> set = new HashSet<>(l);
        assert set.size() == l.size();

        return true;
    }

    public static boolean isAssertEnabled() {
        boolean ret = false;
        assert (ret = true) == true;

        return ret;
    }

    public static void assertAndLog(Logger log, boolean cheapTest, String msg) {
        if (!cheapTest) {
            assertAndLogWithException(log, false, msg, getDebuggingException());
        }
    }

    public static void assertAndLog(SafeLogger log, boolean cheapTest, String msg) {
        if (!cheapTest) {
            assertAndLogWithException(log, false, msg, getDebuggingException());
        }
    }

    public static void assertAndLog(SafeLogger log, boolean cheapTest, String msg, Arg<?>... args) {
        if (!cheapTest) {
            assertAndLogWithException(log, false, msg, getDebuggingException(), args);
        }
    }

    /**
     * @deprecated Use {@link #assertAndLog(Logger, boolean, String)} instead.
     * This will make sure log events go to your logger instead of a hard-to-filter default.
     * (com.palantir.util.AssertUtils)
     */
    @Deprecated
    public static void assertAndLog(boolean cheapTest, String msg) {
        assertAndLog(log, cheapTest, msg);
    }

    public static Exception getDebuggingException() {
        return new SafeRuntimeException(
                "This stack trace is not from a thrown exception. It's provided just for debugging this error.");
    }

    public static void assertAndLog(Logger log, boolean cheapTest, String format, Object... args) {
        if (!cheapTest) {
            assertAndLogWithException(log, false, format, getDebuggingException(), args);
        }
    }

    /**
     * @deprecated Use {@link #assertAndLog(Logger, boolean, String, Object...)} instead.
     * This will make sure log events go to your logger instead of a hard-to-filter default.
     * (com.palantir.util.AssertUtils)
     */
    @Deprecated
    public static void assertAndLog(boolean cheapTest, String format, Object... args) {
        assertAndLog(log, cheapTest, format, args);
    }

    public static void assertAndLogWithException(Logger log, boolean cheapTest, String msg, Throwable t) {
        if (!cheapTest) {
            assertAndLogWithException(log, cheapTest, msg, t, new Object[] {});
        }
    }

    public static void assertAndLogWithException(SafeLogger log, boolean cheapTest, String msg, Throwable t) {
        if (!cheapTest) {
            log.error("An error occurred", SafeArg.of("message", msg), t);
        }
    }

    public static void assertAndLogWithException(
            SafeLogger log, boolean cheapTest, String msg, Throwable t, Arg<?>... args) {
        if (!cheapTest) {
            log.error(
                    "An error occurred",
                    Stream.concat(Stream.of(SafeArg.of("message", msg)), Arrays.stream(args))
                            .collect(Collectors.toList()),
                    t);
        }
    }

    /**
     * @deprecated Use {@link #assertAndLogWithException(Logger, boolean, String, Throwable)} instead.
     * This will make sure log events go to your logger instead of a hard-to-filter default.
     * (com.palantir.util.AssertUtils)
     */
    @Deprecated
    public static void assertAndLogWithException(boolean cheapTest, String msg, Throwable t) {
        assertAndLogWithException(log, cheapTest, msg, t);
    }

    public static void assertAndLogWithException(
            Logger log, boolean cheapTest, String format, Throwable t, Object... args) {
        if (!cheapTest) {
            Object[] newArgs = Arrays.copyOf(args, args.length + 2);
            newArgs[args.length] = SafeArg.of("format", format);
            newArgs[args.length + 1] = t;
            log.error("Assertion with exception!", newArgs);
            assert false : format;
        }
    }

    /**
     * @deprecated Use {@link #assertAndLogWithException(Logger, boolean, String, Throwable, Object...)} instead.
     * This will make sure log events go to your logger instead of a hard-to-filter default.
     * (com.palantir.util.AssertUtils)
     */
    @Deprecated
    public static void assertAndLogWithException(boolean cheapTest, String format, Throwable t, Object... args) {
        assertAndLogWithException(log, cheapTest, format, t, args);
    }
}
