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
package com.palantir.atlasdb.keyvalue.rdbms.utils;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import com.palantir.atlasdb.keyvalue.api.Value;

public class ValueMapper implements ResultSetMapper<Value> {
    private static final ValueMapper instance = new ValueMapper();

    private ValueMapper() {
    }

    @Override
    public Value map(int index, ResultSet r, StatementContext ctx) throws SQLException {
        byte[] content = r.getBytes(Columns.CONTENT.toString());
        long timestamp = TimestampMapper.instance().map(index, r, ctx);
        return Value.create(content, timestamp);
    }

    public static ValueMapper instance() {
        return instance;
    }
}
