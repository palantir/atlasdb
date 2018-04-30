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

/**
 *
 * Represents a results page that is backed by TOKEN.
 *
 * @param <T> the types of results on the page.
 * @param <TOKEN> the token for the page.
 */
public interface TokenBackedBasicResultsPage<T, TOKEN> extends BasicResultsPage<T>, Serializable {
    /**
     * Gets the token for the next page. This method may return null if you are paging from the beginning of time and there are no changes.
     *
     * @return a TOKEN
     */
    TOKEN getTokenForNextPage();
}
