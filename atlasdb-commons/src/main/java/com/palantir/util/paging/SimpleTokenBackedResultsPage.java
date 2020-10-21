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
package com.palantir.util.paging;

import java.io.Serializable;

/**
 *
 * @author manthony
 *
 * @param <T>
 * @param <TOKEN>
 */
public class SimpleTokenBackedResultsPage<T, TOKEN> extends SimpleResultsPage<T>
        implements TokenBackedBasicResultsPage<T, TOKEN>, Serializable {
    private static final long serialVersionUID = 1L;

    private final TOKEN tokenForNextPage;

    /**
     * @param token
     * @param chunks
     * @param moreAvailable
     */
    public SimpleTokenBackedResultsPage(TOKEN token, Iterable<T> chunks, boolean moreAvailable) {
        super(chunks, moreAvailable);
        tokenForNextPage = token;
    }

    /**
     *
     * @param token
     * @param chunks
     */
    public SimpleTokenBackedResultsPage(TOKEN token, Iterable<T> chunks) {
        super(chunks);
        tokenForNextPage = token;
    }

    public static <T, TOKEN> SimpleTokenBackedResultsPage<T, TOKEN> create(TOKEN token, Iterable<T> chunks) {
        return new SimpleTokenBackedResultsPage<T, TOKEN>(token, chunks);
    }

    public static <T, TOKEN> SimpleTokenBackedResultsPage<T, TOKEN> create(
            TOKEN token, Iterable<T> chunks, boolean moreAvailable) {
        return new SimpleTokenBackedResultsPage<T, TOKEN>(token, chunks, moreAvailable);
    }

    @Override
    public TOKEN getTokenForNextPage() {
        return tokenForNextPage;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((tokenForNextPage == null) ? 0 : tokenForNextPage.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj)) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        SimpleTokenBackedResultsPage<?, ?> other = (SimpleTokenBackedResultsPage<?, ?>) obj;
        if (tokenForNextPage == null) {
            if (other.tokenForNextPage != null) {
                return false;
            }
        } else if (!tokenForNextPage.equals(other.tokenForNextPage)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "SimpleTokenBackedResultsPage [tokenForNextPage=" + tokenForNextPage
                + ", results=" + getResults() + ", moreResultsAvailable="
                + moreResultsAvailable() + "]";
    }
}
