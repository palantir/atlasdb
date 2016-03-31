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
package com.palantir.atlasdb.console.module

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.google.common.base.Optional
import com.google.common.collect.ImmutableSet
import com.palantir.atlasdb.api.AtlasDbService
import com.palantir.atlasdb.api.TransactionToken
import com.palantir.atlasdb.config.AtlasDbConfig
import com.palantir.atlasdb.console.AtlasConsoleModule
import com.palantir.atlasdb.console.AtlasConsoleService
import com.palantir.atlasdb.console.AtlasConsoleServiceImpl
import com.palantir.atlasdb.console.AtlasConsoleServiceWrapper
import com.palantir.atlasdb.factory.TransactionManagers
import com.palantir.atlasdb.impl.AtlasDbServiceImpl
import com.palantir.atlasdb.impl.TableMetadataCache
import com.palantir.atlasdb.jackson.AtlasJacksonModule
import com.palantir.atlasdb.table.description.Schema
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager
import groovy.json.JsonBuilder
import groovy.json.JsonOutput
import groovy.transform.CompileStatic
import io.dropwizard.jackson.DiscoverableSubtypeResolver

import javax.net.ssl.SSLSocketFactory

/**
 * Public methods that clients can call within AtlasConsole.
 * Additional public methods contained in Table.groovy and Range.groovy
 */
@CompileStatic
class AtlasCoreModule implements AtlasConsoleModule {
    private static final Map help = [
            'tables': '''\
                         Use tables() to view a list of all tables in AtlasDB accessible
                         by AtlasConsole. Does not include legacy tables. Ex: tables()

                         Use table(<TABLE-NAME>) to get a reference to a table in AtlasDB.
                         Ex: def x = table('stream_value')

                         To retrieve metadata about a table, use <TABLE>.describe(),
                         <TABLE>.isDynamic(), and <TABLE>.columnNames().'''.stripIndent(),
            'transactions': '''\
                         Atomic transactions used to avoid read/write or write/write
                         conflicts. Use startTransaction() to start, currentTransaction()
                         to view all operations in the current transaction, endTransaction()
                         to end and commit the current transaction, and abortTransaction()
                         to end and abort (undo) the current transaction.'''.stripIndent(),
            'pretty print': '''\
                         Print tables and objects in a more human-readable format.

                         Use pp() to print objects. Ex: pp(obj).

                         Use pptable() to print ranges or lists of rows from a table. Ex:
                         pptable(table(<TABLE-NAME>)).getRange(), ['row': [width: 30],
                         'base_object': [width: 50, format: 'json']])'''.stripIndent(),
            'getRange': '''\
                         Retrieve a range of rows from a table. Use getRange() to get all
                         rows from the table, or getRange(args) to get a subset. Ex:
                         <TABLE>.getRange([start: <START-ROW-VALUE>, end: <END-ROW-VALUE>
                         , prefix: <OBJECT-ID-OR-ROW-VALUE>, cols: [<COL-NAMES>]])'''.stripIndent(),
            'getRows': '''\
                         Retrieve one or more rows from a table. Ex:
                         <TABLE>.getRows([ <ROW-VALUE>, <ROW-VALUE> ])'''.stripIndent(),
            'getCells': '''\
                         Retrieve one or more cells from a table, specified by row and
                         column.

                         Ex (for named columns): <TABLE>.getCells([row: <ROW-VALUE>,
                         col: <COL-NAME>])

                         Ex (for dynamic columns): <TABLE>.getCells([row: <ROW-VALUE>,
                         col: <COL-INDEX>])'''.stripIndent(),
            'put': '''\
                         Insert or update one or more rows in a table.

                         Ex (for named columns): <TABLE>.put([row: <ROW-VALUE>, cols:
                         [<COL-NAME>: <COL-VALUE>]])

                         Ex (for dynamic columns): <TABLE>.put([row: <ROW-VALUE>, col:
                         <COL-INDEX>, val: <COL-VALUE>])'''.stripIndent(),
            'delete': '''\
                         Delete one or more rows in a table.
                         Ex: <TABLE>.delete([row: <ROW-VALUE>, cols: [<COL-NAME>,
                          <COL-NAME>]])'''.stripIndent(),
            'connect': '''\
                          Connect to an atlas server using settings from a dropwizard yaml
                          file.
                          Ex: connect(<PATH-TO-YAML-CONFIG>)'''.stripIndent(),
    ]
    private static final int COLUMN_PADDING = 5

    private AtlasConsoleServiceWrapper atlasConsoleServiceWrapper

    private Map bindings;

    public AtlasCoreModule(AtlasConsoleServiceWrapper atlasConsoleServiceWrapper) {
        this.atlasConsoleServiceWrapper = atlasConsoleServiceWrapper
        this.bindings = [
                'table': this.&table,
                'tables': this.&tables,
                'startTransaction': this.&startTransaction,
                'endTransaction': this.&endTransaction,
                'currentTransaction': this.&currentTransaction,
                'abortTransaction': this.&abortTransaction,
                'pp': this.&pp,
                'pptable': this.&pptable,
                'connect': this.&connect,
        ]
    }

    @Override
    Map<String, String> getHelp() {
        return help;
    }

    @Override
    Map<String, Closure> getBindings() {
        return bindings;
    }

    public connect(String yamlFilePath = null) {
        ObjectMapper configMapper = new ObjectMapper(new YAMLFactory());
        configMapper.setSubtypeResolver(new DiscoverableSubtypeResolver());
        JsonNode configRoot = configMapper.readTree(new File(yamlFilePath)).get("atlasdb");
        AtlasDbConfig config = configMapper.treeToValue(configRoot, AtlasDbConfig.class)

        SerializableTransactionManager tm = TransactionManagers.create(config, Optional.<SSLSocketFactory>absent(), ImmutableSet.<Schema>of(),
                new com.palantir.atlasdb.factory.TransactionManagers.Environment() {
                    @Override
                    public void register(Object resource) {
                    }
                }, true)
        TableMetadataCache cache = new TableMetadataCache(tm.getKeyValueService())
        AtlasDbService service = new AtlasDbServiceImpl(tm.getKeyValueService(), tm, cache)
        ObjectMapper serviceMapper = new ObjectMapper()
        serviceMapper.registerModule(new AtlasJacksonModule(cache).createModule())

        AtlasConsoleService atlasConsoleService = new AtlasConsoleServiceImpl(service, serviceMapper)
        atlasConsoleServiceWrapper = AtlasConsoleServiceWrapper.init(atlasConsoleService)
    }

    public tables() {
        def tableList = atlasConsoleServiceWrapper.tables()
        pp(tableList)
        return tableList
    }

    public Table table(String name) {
        new Table(name, atlasConsoleServiceWrapper)
    }

    public startTransaction() {
        atlasConsoleServiceWrapper.startTransaction()
    }

    public endTransaction(TransactionToken token = atlasConsoleServiceWrapper.getTransactionToken()) {
        atlasConsoleServiceWrapper.endTransaction(token)
    }

    public abortTransaction(TransactionToken token = atlasConsoleServiceWrapper.getTransactionToken()) {
        atlasConsoleServiceWrapper.abort(token)
    }

    public currentTransaction() {
        pp(atlasConsoleServiceWrapper.currentTransaction())
    }

    public static void pp(input) {
        def obj
        switch(input) {
            case null:
                println "null"
                return
            case Range:
                obj = input.iterator()
                break
            case Table:
                obj = ((Table)input).getDescription()
                break
            default:
                obj = input
                break
        }
        JsonBuilder builder = new JsonBuilder()
        builder(obj)
        println(builder.toPrettyString())
    }

    /**
     * Print a table or subset of a table in tabular format.
     *
     * @param range     Map of rows to print. Ex: table('ds').getRange()
     *
     * @param cols      Map of columns to print. Column names map to a Map of optional
     *                  parameters for the column:
     *                      width:      Width in number of characters.
     *                      format:     'json' to print in JSON pretty-print format, or 'string'
     *                                  (default) to print as a simple string.
     *
     * @param options   Map of optional parameters for the table:
     *                      maxLines:   Maximum number of lines to print per cell. Defaults to
     *                                  100 if unspecified.
     *                      closure:    Groovy closure (print row only if closure returns true).
     *                                  Defaults to { true }.
     *
     *                  Example query:
     *                      pptable(    table('ds').getRange(),
     *                                  ['datasource': ['width': 60, 'format': 'json']],
     *                                  ['maxLines': 20])
     *
     * @return No return value; print only.
     */
    def pptable(range, Map <String, Map> cols = [:], Map options = [:]) {
        cols.each { key, val ->
            if (!val['width']) {
                val['width'] = key.length() + COLUMN_PADDING
            }
            if (!val['format']) {
                val['format'] = 'string'
            }
        }
        if (!options['maxLines']) {
            options['maxLines'] = 100
        }
        if (!options['closure']) {
            options['closure'] = { true }
        }
        printHeader(cols)
        range.each {
            if ((options['closure'] as Closure).call(it)) {
                printRow(it, cols, options['maxLines'] as int)
            }
        }
    }

    /* Prints column names and two lines separating the header from the table contents.
     */
    private printHeader(Map<String, Map> cols) {
        cols.each { key, val ->
            print key.padRight(val['width'] as int)
        }
        int tableWidth = cols.inject(0) { sum, cur ->
            return sum + (cur.value['width'] as int)
        } as int
        print '\n' + ('=' * tableWidth) + '\n'
    }

    /* Returns a List of Strings, each <width> characters long, of maximum size
     * <maxLines>. The list is obtained by splitting the String <val> on every '\n'
     * character or every <width> characters if those characters do not contain a '\n'.
     */
    private List<String> splitCellString(String val, int width, int maxLines) {
        if (val.length() > (maxLines * (width - COLUMN_PADDING))) {
            val = val.substring(0, (maxLines * (width - COLUMN_PADDING)))
        }

        // Regular expression for inserting a '\n' every <width> characters
        val = val.replaceAll('(.{' + (width - COLUMN_PADDING) + '})', '$1\n')

        if (val == null) {
            return [' ' * width]
        } else {
            List<String> valToArray = val.tokenize('\n')
            return valToArray.collect {
                it.padRight(width)
            }
        }
    }

    /* Returns a nested component from within a row. This can be a cell, a member of
     * a cell, a member of a member of a cell, etc. The component is obtained by
     * splitting <name> on every '.' character and evaluating each level of nesting
     * as either a List or a Map.
     */
    private getColumnComponent(row, String name) {
        def colComponents = name.tokenize('.')
        def col = row
        for (component in colComponents) {
            if (component.isNumber()) {
                def index = component.toInteger()
                col = (col as List)[index]
            } else {
                col = (col as Map)[component]
            }
            if (col == null) {
                break
            }
        }
        return col
    }

    /* Prints the contents of <row>, filtered by the components specified in <cols>.
     * The cells are printed in tabular format with each horizontal row representing
     * a row in the table and each vertical column representing a column or column
     * member specified by the user.
     */

    private printRow(row, Map <String, Map> cols, int maxLines) {
        List<List<String>> colValues = []
        cols.each { key, val ->
            def component = getColumnComponent(row, key)
            if (val['format'] == 'json') {
                component = JsonOutput.prettyPrint(JsonOutput.toJson(component))
            }
            colValues.add(splitCellString(component.toString(),
                                        val['width'] as int,
                                        maxLines))
        }

        int maxArraySize = (colValues.max { it.size() }).size()
        int maxLinesToPrint = (maxLines < maxArraySize) ? maxLines : maxArraySize

        for (int i = 0; i < maxLinesToPrint; i++) {
            colValues.each {
                if (it.size() >= i && it[i]) {
                    print it[i]
                } else {
                    print (' ' * it[0].length())
                }
            }
            print '\n'
        }
        print '\n'
    }

}
