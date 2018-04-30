/*
 * (c) Copyright 2015 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.nexus.db.monitoring.timer;

import java.util.List;

import com.google.common.collect.Lists;

final class CombinedSqlTimer implements SqlTimer {
    private final List<SqlTimer> sqlTimers;

    public CombinedSqlTimer(List<SqlTimer> sqlTimers) {
        this.sqlTimers = sqlTimers;
    }

    @Override
    final public Handle start(String module, String sqlKey, String rawSql) {
        final List<Handle> handles = Lists.newArrayListWithCapacity(sqlTimers.size());
        for (SqlTimer sqlTimer : sqlTimers) {
            handles.add(sqlTimer.start(module, sqlKey, rawSql));
        }
        return () -> {
            for (Handle handle : handles) {
                handle.stop();
            }
        };
    }
}
