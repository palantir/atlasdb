/**
 * Copyright 2015 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.util;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public class AssertUtils {

    public static <T> boolean nonNullItems(Collection<T> c) {
        for (T t : c) {
            if (t == null) return false;
        }

        return true;
    }

    public static <T> boolean assertListElementsUnique(List<T> l) {
        Set<T> set = Sets.newHashSet(l);
        assert set.size() == l.size();

        return true;
    }

    public static boolean isAssertEnabled() {
        boolean ret = false;
        assert (ret = true) == true;

        return ret;
    }

    public static void assertAndLog(boolean cheapTest, String msg) {
        if (!cheapTest) {
            assertAndLogWithException(false, msg, getDebuggingException());
        }
    }

    public static Exception getDebuggingException() {
        return new Exception("This stack trace is not from a thrown exception. It's provided just for debugging this error.");
    }

    public static void assertAndLog(boolean cheapTest, String format, Object... args) {
        if (!cheapTest) {
            assertAndLog(false, String.format(format, args));
        }
    }

    public static void assertAndLogWithException(boolean cheapTest, String msg, Throwable t) {
        if (!cheapTest) {
            LoggerFactory.getLogger(AssertUtils.class).error(msg, t);
            assert false : msg;
        }
    }

    public static void assertAndLogWithException(boolean cheapTest, String format, Throwable t,
            Object... args) {
        if (!cheapTest) {
            assertAndLogWithException(false, String.format(format, args), t);
        }
    }

}
