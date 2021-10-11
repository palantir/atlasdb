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

import com.palantir.logsafe.exceptions.SafeIllegalStateException;

import javax.management.JMException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import java.io.IOException;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadInfo;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SuppressWarnings({
        "checkstyle",
        "StringSplitter" // There but for the grace of God go I
})
// WARNING: This class was copied verbatim from an internal product. We are aware that the code quality is not great
// and we lack tests, however this is covered by testing in the internal product
// DO NOT CHANGE THIS CLASS!
public final class StackTraceUtils {
    private StackTraceUtils() {
        // utility
    }

    private static final ObjectName THREAD_MXBEAN;
    private static final ObjectName MEMORY_MXBEAN;

    private static final String INDENT = "    ";
    public static final String LINE_ENDING = "\n";

    static {
        try {
            THREAD_MXBEAN = new ObjectName(ManagementFactory.THREAD_MXBEAN_NAME);
        } catch (Exception e) {
            throw new SafeIllegalStateException("Failed to initialize thread MXBean name.");
        }
        try {
            MEMORY_MXBEAN = new ObjectName(ManagementFactory.MEMORY_MXBEAN_NAME);
        } catch (Exception e) {
            throw new SafeIllegalStateException("Failed to initialize memory MXBean name.");
        }
    }

    public static String[] getStackTraceForConnection(MBeanServerConnection connection)
            throws JMException, IOException {
        return getStackTraceForConnection(connection, false);
    }

    @SuppressWarnings("BadAssert") // performance sensitive
    public static String[] getStackTraceForConnection(MBeanServerConnection connection, boolean redact)
            throws JMException, IOException {
        long[] threadIDs = (long[]) connection.getAttribute(THREAD_MXBEAN, "AllThreadIds");
        MemoryUsage.from((CompositeData) connection.getAttribute(MEMORY_MXBEAN, "HeapMemoryUsage"));

        Class<?> monitorInfoClass = null;
        Method getLockedMonitorsMethod = null;
        Method getLockedStackDepthMethod = null;
        try {
            monitorInfoClass = Class.forName("java.lang.management.MonitorInfo");
            getLockedMonitorsMethod = ThreadInfo.class.getMethod("getLockedMonitors");
            getLockedStackDepthMethod = monitorInfoClass.getMethod("getLockedStackDepth");
        } catch (ClassNotFoundException e) {
            // ignored
            /**/
        } catch (NoSuchMethodException e) {
            // ignored
            /**/
        }
        boolean java16 = monitorInfoClass != null;

        CompositeData[] threadData = null;
        if (java16) {
            threadData = (CompositeData[])
                    connection.invoke(THREAD_MXBEAN, "dumpAllThreads", new Object[] {true, true}, new String[] {
                        boolean.class.getName(), boolean.class.getName()
                    });
        } else {
            threadData = (CompositeData[]) connection.invoke(
                    THREAD_MXBEAN, "getThreadInfo", new Object[] {threadIDs, Integer.MAX_VALUE}, new String[] {
                        long[].class.getName(), int.class.getName()
                    });
        }

        String[] resultData = new String[threadData.length];
        for (int i = 0; i < resultData.length; i++) {
            ThreadInfo info = ThreadInfo.from(threadData[i]);
            Object[] lockedMonitors = new Object[0];
            if (java16) {
                try {
                    lockedMonitors = (Object[]) getLockedMonitorsMethod.invoke(info);
                } catch (InvocationTargetException e) {
                    assert false : "anonymous assert FF3FEF";
                } catch (IllegalAccessException e) {
                    assert false : "anonymous assert 97D861";
                }
            }
            int[] stackDepths = new int[lockedMonitors.length];
            if (lockedMonitors.length != 0) {
                try {
                    for (int j = 0; j < stackDepths.length; j++) {
                        stackDepths[j] = ((Integer) getLockedStackDepthMethod.invoke(lockedMonitors[j])).intValue();
                    }
                } catch (InvocationTargetException e) {
                    /* if lockedMonitors is not empty, then getLockedMonitors exists,
                     * ergo monitorInfo exists and we should never end up here
                     * (or in the next catch block)
                     */
                    assert false : "anonymous assert 11482A";
                } catch (IllegalAccessException e) {
                    assert false : "anonymous assert FBEB54";
                }
            }

            if (redact) {
                resultData[i] = redact(threadToString(info, lockedMonitors, stackDepths));
            } else {
                resultData[i] = threadToString(info, lockedMonitors, stackDepths);
            }
        }

        return resultData;
    }

    public static final Pattern IP_ADDRESS_REGEX = Pattern.compile("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}");
    public static final String IP_ADDRESS_REDACTED = "[REDACTED IP ADDRESS]";

    public static String redact(CharSequence trace) {
        Matcher matcher = IP_ADDRESS_REGEX.matcher(trace);
        return matcher.replaceAll(IP_ADDRESS_REDACTED);
    }

    public static String threadToString(ThreadInfo info, Object[] lockedMonitors, int[] stackDepths) {
        StringBuilder sb = new StringBuilder();
        threadHeaderToString(info, sb);

        int curLock = 0;
        StackTraceElement[] stackTrace = info.getStackTrace();
        for (int i = 0; i < stackTrace.length && i < MAX_FRAMES; i++) {
            StackTraceElement ste = stackTrace[i];
            sb.append(INDENT + "at " + ste.toString());
            sb.append(LINE_ENDING);

            while (curLock < stackDepths.length && i == stackDepths[curLock]) {
                String[] lockName = lockedMonitors[curLock].toString().split("@");
                sb.append(INDENT + " - locked <" + lockName[1] + "> (a " + lockName[0] + ")");
                sb.append(LINE_ENDING);
                curLock++;
            }
        }
        sb.append(LINE_ENDING);

        return sb.toString();
    }

    /**
     * Print out a thread dump header. This method was copied from Thread Dump Analyzer (and slightly modified)
     * http://java.net/projects/tda/sources/svn/content/trunk/tda/src/java/com/pironet/tda/jconsole/MBeanDumper.java?rev=638
     */
    private static void threadHeaderToString(ThreadInfo ti, StringBuilder dump) {
        // The thread priority here is a lie, but automated thread dump analyzer samurai
        // requires it and ThreadInfo does not provide it. The nid is also a lie, but
        // Thread Dump Analyzer requires it.
        dump.append("\"" + ti.getThreadName() + "\" prio=10 tid=0x" + Long.toHexString(ti.getThreadId()) + " nid="
                + ti.getThreadId());

        // These are a best effort match to what kill -3 would report as the status. It's not perfect, but
        // samurai will parse it correctly.
        switch (ti.getThreadState()) {
            case RUNNABLE:
                dump.append(" runnable");
                break;
            case BLOCKED:
                dump.append(" waiting for monitor entry");
                break;
            case WAITING:
            case TIMED_WAITING:
                if (ti.getStackTrace().length > 0) {
                    StackTraceElement e = ti.getStackTrace()[0];
                    if (e.getClassName().equals("java.lang.Object")
                            && e.getMethodName().equals("wait")) {
                        dump.append(" in Object.wait()");
                    } else if (e.getClassName().equals("java.lang.Thread")
                            && e.getMethodName().equals("sleep")) {
                        dump.append(" suspended");
                    } else {
                        dump.append(" waiting on condition");
                    }
                } else {
                    dump.append(" waiting on condition");
                }
                break;
            case NEW:
                dump.append(" new");
                break;
            case TERMINATED:
                dump.append(" terminated");
                break;
        }
        dump.append(LINE_ENDING + "  java.lang.Thread.State: " + ti.getThreadState());

        if (ti.getLockName() != null) {
            String[] lockName = ti.getLockName().split("@");
            if (ti.getThreadState() == Thread.State.BLOCKED) {
                dump.append(LINE_ENDING + INDENT + " - waiting to lock <" + lockName[1] + "> (a " + lockName[0] + ")");
            } else {
                dump.append(LINE_ENDING + INDENT + " - waiting on <" + lockName[1] + "> (a " + lockName[0] + ")");
                dump.append(LINE_ENDING + INDENT + " - locked <" + lockName[1] + "> (a " + lockName[0] + ")");
            }
        }
        if (ti.isSuspended()) {
            dump.append(" (suspended)");
        }
        if (ti.isInNative()) {
            dump.append(" (running in native)");
        }
        dump.append(LINE_ENDING);
        if (ti.getLockOwnerName() != null) {
            dump.append(INDENT + " owned by " + ti.getLockOwnerName() + " tid=" + ti.getLockOwnerId());
            dump.append(LINE_ENDING);
        }
    }

    private static final int MAX_FRAMES = Integer.MAX_VALUE;

    private static final int POINTS_PER_LINE = 1;
    private static final int POINTS_PER_PALANTIR = 10;

    private static int score(String trace) {
        int score = 0;
        int index = trace.indexOf("com.palantir");
        while (index != -1) {
            score += POINTS_PER_PALANTIR;
            int nextStartingIndex = index + 1;
            index = trace.indexOf("com.palantir", nextStartingIndex);
        }
        score += POINTS_PER_LINE * trace.split(LINE_ENDING).length;
        return score;
    }

    private static final class StackTraceComparator implements Comparator<String>, Serializable {
        private static final long serialVersionUID = 1L;

        // higher scores come earlier
        @Override
        public int compare(String s1, String s2) {
            return score(s2) - score(s1);
        }
    }

    public static String[] splitStackTrace(String traces) {
        String[] split = traces.split("\n\\s*\n");
        if (split.length == 1) {
            split = traces.split("\r\\s*\r");
        }
        if (split.length == 1) {
            split = traces.split("\r\n\\s*\r\n");
        }
        List<String> filteredTraces = new ArrayList<String>();
        for (String trace : split) {
            if (!trace.replaceAll("\\s", "").isEmpty()) {
                filteredTraces.add(trace + "\r\r");
            }
        }
        return filteredTraces.toArray(new String[0]);
    }

    public static String processTrace(String serverName, String[] traces, boolean abridged) {
        StackTraceBuilder stackTraceBuilder = new StackTraceBuilder(serverName, abridged);
        if (traces == null) {
            return stackTraceBuilder.getStackTraceForNoIncorporatedTraces();
        }
        Arrays.sort(traces, new StackTraceComparator());
        for (String trace : traces) {
            stackTraceBuilder.incorporateTrace(trace);
        }
        return stackTraceBuilder.getResultStackTrace();
    }

    /*******************************************************************************************
     * c/p from TextUtils
     */
    private static Map<String, String> plurals = new HashMap<String, String>();

    static {
        /* don't need special plurals */
    }

    /**
     * This function pluralizes the given text and now accounts for three capitalization cases: lower case, Camel Case, and ALL CAPS.
     * It converts the text to lower case first and looks it up in the plurals dictionary (which we assume to be all lower case now).
     * If it does not exist, it simply appends a "s" to the word.  Then it converts the capitalization.  Also see TextUtilText.testPluralizeWithCaps().
     */
    public static String pluralize(String text) {
        if (text == null || "".equals(text)) {
            return "";
        }
        Boolean capsType = null; // null for all lower case, false for Camel Case, true for ALL UPPER CASE
        if (text.length() > 0) {
            char[] textArray = text.toCharArray();
            if (Character.isUpperCase(textArray[0])) {
                capsType = false; // Camel Case
                if (text.equals(text.toUpperCase())) {
                    capsType = true; // UPPER CASE
                }
            }
        }
        String lowerText = text.toLowerCase();
        String plural = plurals.get(lowerText) == null ? (lowerText + "s") : plurals.get(lowerText);
        if (capsType == null) {
            return plural; // lower case
        } else if (capsType == false) {
            if (plural != null && plural.length() > 0) {
                return Character.toUpperCase(plural.charAt(0)) + plural.substring(1); // Camel Case
            } else {
                return plural;
            }
        } else {
            return plural.toUpperCase(); // UPPER CASE
        }
    }

    /**
     * Pluralizes a word if count != 1. In the future, could use a
     * dictionary-based (perhaps even locale-sensitive) approach to get proper
     * pluralization for many words.
     */
    public static String pluralizeWord(String s, int count) {
        if (count == 1) {
            return s;
        }

        return pluralize(s);
    }

    private static class StackTraceBuilder {
        private String lineEnding;
        private boolean abridged;
        private String header;
        private String subheader;
        private List<String> summarizedNames;
        private List<String> fullTraces;
        private int boringCount;
        private int incorporatedTracesCount;

        private static final int PRINT_FULL_THRESHOLD = 2 * POINTS_PER_PALANTIR;
        private static final int PRINT_SUMMARY_THRESHOLD = POINTS_PER_PALANTIR;

        StackTraceBuilder(String serverName, boolean abridged) {
            this.summarizedNames = new ArrayList<String>();
            this.fullTraces = new ArrayList<String>();
            this.boringCount = 0;
            this.abridged = abridged;
            this.incorporatedTracesCount = 0;

            // subheader is used by the samurai automated thread dump analyzer to start parsing a thread dump.
            // The exact phrase "Full thread dump" must appear.
            this.subheader = "Full thread dump of ";
            this.lineEnding = LINE_ENDING + LINE_ENDING;
            this.header = createHeader(serverName);
        }

        public void incorporateTrace(String trace) {
            incorporatedTracesCount++;
            if (!abridged) {
                fullTraces.add(trace);
            } else {
                updateUsingSummarizationRules(trace);
            }
        }

        public String getResultStackTrace() {
            StringBuilder resultStackTrace = new StringBuilder();
            for (String trace : fullTraces) {
                resultStackTrace.append(trace);
            }
            Collections.sort(summarizedNames);
            if (abridged) {
                resultStackTrace.append(lineEnding);
                appendSummarizedNamesToResult(resultStackTrace);
            }
            int dumpCount = incorporatedTracesCount - summarizedNames.size() - boringCount;
            return header + subheader + dumpCount + " " + pluralizeWord("thread", dumpCount) + ":" + lineEnding
                    + resultStackTrace.toString();
        }

        public String getStackTraceForNoIncorporatedTraces() {
            return header + "An error occurred: no stack trace was taken.";
        }

        private void appendSummarizedNamesToResult(StringBuilder resultStackTrace) {
            resultStackTrace.append(
                    summarizedNames.size() + " " + pluralizeWord("thread", summarizedNames.size()) + " summarized");
            if (!summarizedNames.isEmpty()) {
                resultStackTrace.append(": ");
                resultStackTrace.append(lineEnding);
                for (int i = 0; i < summarizedNames.size() - 1; i++) {
                    String currSummarizedName = summarizedNames.get(i);
                    resultStackTrace.append("\t" + currSummarizedName + "\n");
                }
                String lastSummarizedName = summarizedNames.get(summarizedNames.size() - 1);
                resultStackTrace.append("\t" + lastSummarizedName);
            }
            resultStackTrace.append(lineEnding + boringCount + " " + pluralizeWord("thread", boringCount) + " omitted");
        }

        private String createHeader(String serverName) {
            header = "Trace of " + serverName + " taken at " + Instant.now().toString();
            StringBuilder dashes = new StringBuilder();
            for (int i = 0; i < header.length(); i++) {
                dashes.append("-");
            }
            header = dashes + LINE_ENDING + header + LINE_ENDING + dashes + this.lineEnding;
            return header;
        }

        private void updateUsingSummarizationRules(String trace) {
            if (isIdleElasticSearchThread(trace)) {
                insertTraceIntoSummarizedNames(trace);
            } else {
                int traceScore = StackTraceUtils.score(trace);
                if (traceScore >= PRINT_FULL_THRESHOLD) {
                    fullTraces.add(trace);
                } else if (traceScore >= PRINT_SUMMARY_THRESHOLD) {
                    insertTraceIntoSummarizedNames(trace);
                } else {
                    boringCount++;
                }
            }
        }

        private void insertTraceIntoSummarizedNames(String trace) {
            String firstLine = getFirstLineOfTrace(trace);
            summarizedNames.add(firstLine);
        }

        private boolean isIdleElasticSearchThread(String trace) {
            String firstLine = getFirstLineOfTrace(trace);
            boolean isElasticSearchThread = firstLine.contains("elasticsearch");
            boolean isIdleThread =
                    trace.contains("EPollArrayWrapper.poll") && trace.contains("EPollArrayWrapper.epollWait");
            return isElasticSearchThread && isIdleThread;
        }

        private String getFirstLineOfTrace(String trace) {
            String[] traceLines = trace.split("\n");
            return traceLines[0];
        }
    }
}
