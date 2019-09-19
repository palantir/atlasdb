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
package com.palantir.util.debug;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import javax.management.JMException;

@SuppressWarnings("checkstyle")
// WARNING: This class was copied verbatim from an internal product. We are aware that the code quality is not great
// and we lack tests, however this is covered by testing in the internal product
// DO NOT CHANGE THIS CLASS!
public class ThreadDumps {
    public static String programmaticThreadDump() {
        String serverName = "Stack Trace"; //$NON-NLS-1$
        try {
            return StackTraceUtils.processTrace(serverName, //$NON-NLS-1$
                    StackTraceUtils.getStackTraceForConnection(ManagementFactory.getPlatformMBeanServer()),
                    false);
        } catch (JMException e) {
            return fallbackThreadDump(serverName);
        } catch (IOException e) {
            return fallbackThreadDump(serverName);
        }
    }

    private static String fallbackThreadDump(String dumpName) {
        StringBuilder dump = new StringBuilder();
        dump.append(dumpName);
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        for (ThreadInfo info : threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds(), 500)) {
            if (info == null) {
                continue;
            }
            dump.append('"').append(info.getThreadName()).append("\" ");
            dump.append("\n   java.lang.Thread.State:").append(info.getThreadState());

            if (info.getLockName() != null) {
                switch (info.getThreadState()) {
                    case BLOCKED:
                        dump.append("\r\n\t-  blocked on " + info.getLockName()); //$NON-NLS-1$
                        break;
                    case WAITING:
                        dump.append("\r\n\t-  waiting on " + info.getLockName()); //$NON-NLS-1$
                        break;
                    case TIMED_WAITING:
                        dump.append("\r\n\t-  waiting on " + info.getLockName()); //$NON-NLS-1$
                        break;
                    default:
                        break;
                }
            }

            for (StackTraceElement stackTraceElement : info.getStackTrace()) {
                dump.append("\n        at ").append(stackTraceElement);
            }
            dump.append("\n\n");
        }
        return dump.toString();
    }
}
