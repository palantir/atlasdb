/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.api;

import com.palantir.atlasdb.keyvalue.api.Cell;
import java.util.function.LongFunction;

/**
 * Curious creature, but it allows the user to delay deciding the particular cell to write to until the actual commit,
 * at which point the user will be provided with a unique long value that can be used to generate the cell.
 *
 * This is a very specific feature that not many people probably require, or should try to use.
 *
 */
@FunctionalInterface
public interface DelayedWrite extends LongFunction<Cell> {}
