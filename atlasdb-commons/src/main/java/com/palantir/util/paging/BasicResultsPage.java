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
package com.palantir.util.paging;

import java.io.Serializable;
import java.util.List;

/**
 * Provides a minimal paging interface.
 *
 * @param <T> the type of item being paged
 */
public interface BasicResultsPage<T> extends Serializable {

    /**
     * Returns the results in the current page.
     * @return list of results
     */
    List<T> getResults();

    /**
     * Identifies if more results are available.
     * @return true if there are more pages, otherwise false
     */
    boolean moreResultsAvailable();
}
