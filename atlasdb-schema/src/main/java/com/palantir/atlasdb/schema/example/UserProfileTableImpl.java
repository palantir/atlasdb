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
package com.palantir.atlasdb.schema.example;

import java.io.IOException;
import java.util.SortedMap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Longs;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.transaction.api.Transaction;

public class UserProfileTableImpl implements UserProfileTable {
	
	private static final ObjectMapper mapper = new ObjectMapper();
	
	private static final String TABLE_NAME = "user_profile";
	
	private static final byte[] userProfileColumn = {0};
	private static final ColumnSelection userProfileColumnSelection
		= ColumnSelection.create(ImmutableSet.of(userProfileColumn));
	
	private final Transaction t;
	
	public UserProfileTableImpl(Transaction t) {
		this.t = t;
	}
	
	private byte[] getKey(long userId) {
		return Longs.toByteArray(userId);
	}
	
	private Iterable<byte[]> getKeys(Iterable<Long> userIds) {
		return Iterables.transform(userIds, new Function<Long,byte[]>(){
			@Override
			public byte[] apply(Long input) {
				return getKey(input);
			}
		});
	}

	@Override
	public UserProfile getUserProfile(long userId) {
		ImmutableSet<byte[]> userIds = ImmutableSet.of(getKey(userId));

		SortedMap<byte[], RowResult<byte[]>> rows = t.getRows(TABLE_NAME, userIds, userProfileColumnSelection);
		RowResult<byte[]> rowResult = rows.get(rows.lastKey());
		return asValueQuietly(rowResult.getOnlyColumnValue());
	}
	
	private UserProfile asValueQuietly(byte[] bytes) {
        try {
            return mapper.readValue(bytes, UserProfile.class);
        } catch (IOException e) {
            return null;
        }
    }
}
