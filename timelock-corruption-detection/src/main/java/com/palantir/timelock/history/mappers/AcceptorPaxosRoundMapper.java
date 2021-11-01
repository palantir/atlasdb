/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.timelock.history.mappers;

import com.google.common.collect.Maps;
import com.palantir.paxos.PaxosAcceptorState;
import com.palantir.timelock.history.PaxosAcceptorData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

public class AcceptorPaxosRoundMapper implements RowMapper<Map.Entry<Long, PaxosAcceptorData>> {
    @Override
    public Map.Entry<Long, PaxosAcceptorData> map(ResultSet rs, StatementContext _ctx) throws SQLException {
        PaxosAcceptorState value = PaxosAcceptorState.BYTES_HYDRATOR.hydrateFromBytes(rs.getBytes("val"));
        return Maps.immutableEntry(rs.getLong("seq"), mapPaxosAcceptorStateToSerializableData(value));
    }

    private PaxosAcceptorData mapPaxosAcceptorStateToSerializableData(PaxosAcceptorState state) {
        return PaxosAcceptorData.builder()
                .lastPromisedId(Optional.ofNullable(state.getLastPromisedId()))
                .lastAcceptedId(Optional.ofNullable(state.getLastAcceptedId()))
                .version(state.getVersion())
                .lastAcceptedValue(Optional.ofNullable(state.getLastAcceptedValue()))
                .build();
    }
}
