/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.timestamp;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;

import org.junit.Test;

public class TimestampRangeTest {
    @Test
    public void shouldHaveASizeOf1WhenCreatedWithTheSameUpperAndLowerBounds() {
        assertThat(TimestampRange.createInclusiveRange(10, 10).size(), is(1L));
    }

    @Test
    public void shouldHaveASizeThatInludesBothEndpoints() {
        assertThat(TimestampRange.createInclusiveRange(10, 12).size(), is(3L));
    }

    @Test
    public void shouldHaveTheSameSizeWhicheverWayRoundTheBoundsArePassed() {
        long upper = 10;
        long lower = 1;
        TimestampRange lowerUpper = TimestampRange.createInclusiveRange(lower, upper);
        TimestampRange upperLower = TimestampRange.createInclusiveRange(upper, lower);

        assertThat(lowerUpper.size(), is(equalTo(upperLower.size())));
    }
}
