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

import static java.lang.Math.min;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class PageDrainerTest {
    private static final int PAGE_SIZE = 10;
    private static final int INCOMPLETE_PAGE_SIZE = 5;

    private static class SimplePager implements PageGetter<Integer> {
        private final int pageSize;
        private final int limit;

        protected SimplePager(int pageSize, int limit) {
            this.pageSize = pageSize;
            this.limit = limit;
        }

        @Override
        public List<Integer> getFirstPage() {
            return getIntegers(1);
        }

        @Override
        public List<Integer> getNextPage(List<Integer> currentPage) {
            return getIntegers(Iterables.getLast(currentPage, 0) + 1);
        }

        @Override
        public int getPageSize() {
            return pageSize;
        }

        private List<Integer> getIntegers(int start) {
            List<Integer> page = new ArrayList<>();
            for (Integer i = start; i <= min(i + pageSize, limit); i++) {
                page.add(i);
            }
            return page;
        }
    }

    @Test
    public void getsEmptyListIfNoResults() {
        assertGetsResultsUpTo(0);
    }

    @Test
    public void getsOnePartialPage() {
        assertGetsResultsUpTo(INCOMPLETE_PAGE_SIZE);
    }

    @Test
    public void getsOneFullPage() {
        assertGetsResultsUpTo(PAGE_SIZE);
    }

    @Test
    public void getsMultiplePagesWithLastOnePartial() {
        assertGetsResultsUpTo(PAGE_SIZE * 2 + INCOMPLETE_PAGE_SIZE);
    }

    @Test
    public void getsMultiplePagesWithLastOneComplete() {
        assertGetsResultsUpTo(PAGE_SIZE * 3);
    }

    private void assertGetsResultsUpTo(int limit) {
        PageDrainer<Integer> pageDrainer = new PageDrainer<Integer>(new SimplePager(PAGE_SIZE, limit));
        List<Integer> pages = pageDrainer.drainAllPages();
        assertThat(pages).hasSize(limit);
    }
}
