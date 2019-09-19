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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.io.Serializable;
import java.util.Collections;
import javax.annotation.concurrent.Immutable;

/**
 *
 * Supplies a simple result page.
 *
 *  @param <T> the type of item being paged
 */
@Immutable
public class SimpleResultsPage<T> implements BasicResultsPage<T>, Serializable {
    private static final long serialVersionUID = 2L;

    private final ImmutableList<T> chunks;
    private final boolean moreAvailable;

    /**
     * Constructs an empty results page.
     *
     * @param <T> the type of result
     * @return a page
     */
    public static <T> SimpleResultsPage<T> emptyPage() {
        return new SimpleResultsPage<T>(Collections.<T>emptyList(), false);
    }

    /**
     * Constructs a page of iterable chunks of results.
     *
     * @param chunks an iterable chunk of results
     * @param moreAvailable indicates whether more is available
     */
    public SimpleResultsPage(Iterable<T> chunks, boolean moreAvailable) {
        this.chunks = ImmutableList.copyOf(chunks);
        this.moreAvailable = moreAvailable;
    }

    /**
     * Constructs a page of iterable chunks.
     *
     * @param chunks a set of chunks
     */
    public SimpleResultsPage(Iterable<T> chunks) {
        this(chunks, !Iterables.isEmpty(chunks));
    }

    public static <T> SimpleResultsPage<T> create(Iterable<T> chunks) {
        return new SimpleResultsPage<T>(chunks);
    }

    public static <T> SimpleResultsPage<T> create(Iterable<T> chunks, boolean moreAvailable) {
        return new SimpleResultsPage<T>(chunks, moreAvailable);
    }

    @Override
    public ImmutableList<T> getResults() {
        return chunks;
    }

    @Override
    public boolean moreResultsAvailable() {
        return moreAvailable;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((chunks == null) ? 0 : chunks.hashCode());
        result = prime * result + (moreAvailable ? 1231 : 1237);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SimpleResultsPage<?> other = (SimpleResultsPage<?>) obj;
        if (chunks == null) {
            if (other.chunks != null)
                return false;
        } else if (!chunks.equals(other.chunks))
            return false;
        if (moreAvailable != other.moreAvailable)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "SimpleResultsPage [chunks=" + chunks + ", moreAvailable=" + moreAvailable + "]";
    }
}
