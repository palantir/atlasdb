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
package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import com.google.common.collect.Lists;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.nexus.db.sql.BasicSQLUtils;
import java.util.Collection;
import java.util.List;

public final class WhereClauses {
    private final List<String> clauses;
    private final List<Object> arguments;

    private WhereClauses(List<String> clauses, List<Object> arguments) {
        this.clauses = clauses;
        this.arguments = arguments;
    }

    public static WhereClauses create(String tableIdentifier, RangeRequest request, String... clauses) {
        List<String> extraWhereClauses = Lists.newArrayList(clauses);

        byte[] start = request.getStartInclusive();
        byte[] end = request.getEndExclusive();
        Collection<byte[]> cols = request.getColumnNames();

        List<Object> args = Lists.newArrayListWithCapacity(2 + cols.size());
        List<String> whereClauses = Lists.newArrayListWithCapacity(3 + extraWhereClauses.size());

        if (start.length > 0) {
            whereClauses.add(tableIdentifier + (request.isReverse() ? ".row_name <= ?" : ".row_name >= ?"));
            args.add(start);
        }
        if (end.length > 0) {
            whereClauses.add(tableIdentifier + (request.isReverse() ? ".row_name > ?" : ".row_name < ?"));
            args.add(end);
        }
        if (!cols.isEmpty()) {
            whereClauses.add(tableIdentifier + ".col_name IN (" + BasicSQLUtils.nArguments(cols.size()) + ")");
            args.addAll(cols);
        }

        whereClauses.addAll(extraWhereClauses);

        return new WhereClauses(whereClauses, args);
    }

    public List<Object> getArguments() {
        return arguments;
    }

    public List<String> getClauses() {
        return clauses;
    }
}
