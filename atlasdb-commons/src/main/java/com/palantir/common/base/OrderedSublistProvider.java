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
package com.palantir.common.base;

import java.util.List;

import com.palantir.common.annotation.Inclusive;

/**
 * Provides sublists of a list of T's, where each T is identified by a long.
 *
 * This is useful with, for instance, BatchingVisitables.getOrderedVisitableUsingSublists()
 * or OrderedSublistProviders.countApproximateNumIds().
 */
public interface OrderedSublistProvider<T> {
    /**
     * Gets a sorted list of the next maxNumResults T's identified by at least startID.
     * Adjacent duplicates are allowed.
     *
     * Note that if there are maxNumResults results available, this method MUST return a list of
     * that size. Otherwise, we assume that this is the end of the list.
     */
    List<T> getBatchAllowDuplicates(@Inclusive long startId, int maxNumResults);
}
