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
package com.palantir.util.sql;


import javax.management.MXBean;

@MXBean
public interface SqlCallStatsMBean {
    public String getQueryName();
    public String getRawSql();

    public void clearStats();
    public long getTotalTime();
    public long getTotalCalls();
    public long getTimePerCallInMillis();
    public double getStandardDeviationInMillis();
    public long getMaxCallTime();
    public long getMinCallTime();
    public double getPercentileMillis(double perc);
    public double getPercentCallsFinishedInMillis(int millis);
    public double getMedianTimeRequestInMillis();

    // The following are redundant with getPercentileMillis, but they should
    // make information easier to browse from jconsole.
    public double get25thPercentileCallTimeMillis();
    public double get75thPercentileCallTimeMillis();
}
