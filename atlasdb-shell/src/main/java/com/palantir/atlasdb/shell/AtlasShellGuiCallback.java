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
package com.palantir.atlasdb.shell;

import java.util.List;

import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.table.description.TableMetadata;

/**
 * The callback ultimately called by the #view method in Ruby. This is an interface because behavior
 * must vary between the GUI and terminal-only version of the shell program.
 */
public interface AtlasShellGuiCallback {

    /**
     * Callback to display a graphical representation of the rows, optionally displaying only
     * specified columns matching on short or long columns names. A null or empty columns value will
     * display all columns.
     */
    void graphicalView(TableMetadata tableMetadata,
              String tableName,
              List<String> columns,
              List<RowResult<byte[]>> rows,
              boolean limitedResults);

    boolean isGraphicalViewEnabled();

    void clear();
}
