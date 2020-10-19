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
import com.google.common.collect.Lists;
import java.util.List;

public class PageDrainer<T> {
    private final PageGetter<T> pageGetter;

    public PageDrainer(PageGetter<T> pageGetter) {
        this.pageGetter = pageGetter;
    }

    public List<T> drainAllPages() {
        List<T> pages = Lists.newArrayList();
        List<T> currentPage = pageGetter.getFirstPage();
        pages.addAll(currentPage);
        while (hasNextPage(currentPage)) {
            currentPage = pageGetter.getNextPage(ImmutableList.copyOf(currentPage));
            pages.addAll(currentPage);
        }

        return pages;
    }

    private boolean hasNextPage(List<T> currentPage) {
        return currentPage.size() >= pageGetter.getPageSize();
    }
}
