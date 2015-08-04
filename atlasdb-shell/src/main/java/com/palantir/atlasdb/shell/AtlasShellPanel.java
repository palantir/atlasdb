/**
 * // Copyright 2015 Palantir Technologies
 * //
 * // Licensed under the BSD-3 License (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * // http://opensource.org/licenses/BSD-3-Clause
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */
package com.palantir.atlasdb.shell;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Font;
import java.awt.Insets;
import java.awt.Point;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;

import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.JComponent;
import javax.swing.JEditorPane;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.JTextPane;
import javax.swing.KeyStroke;
import javax.swing.ScrollPaneConstants;
import javax.swing.SwingUtilities;
import javax.swing.border.EmptyBorder;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.TableColumnModelEvent;
import javax.swing.event.TableColumnModelListener;
import javax.swing.table.TableColumn;

import org.jruby.demo.TextAreaReadline;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.shell.audit.AuditLoggingConnection;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.ptoss.util.Throwables;


public class AtlasShellPanel extends JPanel implements AtlasShellGuiCallback {
    private static final long serialVersionUID = 1L;
    private final AtlasShellInterruptCallback interruptCallback;
    private final JEditorPane editorPane;
    private final TextAreaReadline textArea;
    private final JTabbedPane viewPane;

    @SuppressWarnings("serial")
    public AtlasShellPanel() {
        assert SwingUtilities.isEventDispatchThread();
        interruptCallback = new AtlasShellInterruptCallback();
        editorPane = new JTextPane() {
            @Override
            public void copy() {
                // This is a bit silly, but it allows Ctrl+c to simultaneously
                // (a) Act like a "SIGINT" and interrupt any currently running operations (if any)
                // (b) Copy currently selected text to the clipboard (which is default behavior)
                interruptCallback.interrupt();
                super.copy();
            }
        };
        editorPane.setMargin(new Insets(8, 8, 8, 8));
        editorPane.setCaretColor(new Color(0xa4, 0x00, 0x00));
        editorPane.setBackground(new Color(0xf2, 0xf2, 0xf2));
        editorPane.setForeground(new Color(0xa4, 0x00, 0x00));
        editorPane.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 12));
        textArea = new TextAreaReadline(editorPane);
        JScrollPane textScrollPane = new JScrollPane();
        textScrollPane.setViewportView(editorPane);
        textScrollPane.setBorder(BorderFactory.createLineBorder(Color.darkGray));
        viewPane = new JTabbedPane();
        viewPane.setBorder(new EmptyBorder(2, 5, 2, 2));
        JSplitPane splitPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT, textScrollPane, viewPane);
        splitPane.setOneTouchExpandable(true);
        splitPane.setDividerLocation(500);
        splitPane.setResizeWeight(1.0);
        setLayout(new BorderLayout());
        add(splitPane, BorderLayout.CENTER);
        validate();
    }

    @SuppressWarnings("deprecation")
    public void run(final AtlasShellRubyScriptlet scriptlet, AuditLoggingConnection auditLogger, AtlasShellContextFactory atlasShellContextFactory) {
        assert SwingUtilities.isEventDispatchThread();
        AtlasShellRuby atlasShellRuby = AtlasShellRuby.createInteractive(
                scriptlet,
                textArea.getInputStream(),
                textArea.getOutputStream(),
                textArea.getOutputStream(),
                interruptCallback,
                auditLogger,
                atlasShellContextFactory);
        atlasShellRuby.setAtlasShellTableViewer(new AtlasShellGuiCallback() {
            @Override
            public void graphicalView(final TableMetadata tableMetadata,
                             final String tableName,
                             final List<String> columns,
                             final List<RowResult<byte[]>> rows,
                             final boolean limitedResults) {
                assert !SwingUtilities.isEventDispatchThread();
                try {
                    SwingUtilities.invokeAndWait(PTExecutors.wrap(new Runnable() {
                        @Override
                        public void run() {
                            AtlasShellPanel.this.graphicalView(
                                    tableMetadata,
                                    tableName,
                                    columns,
                                    rows,
                                    limitedResults);
                        }
                    }));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (InvocationTargetException e) {
                    Throwables.throwUncheckedException(e);
                }
            }

            @Override
            public boolean isGraphicalViewEnabled() {
                return true;
            }

            @Override
            public void clear() {
                editorPane.setText("");
            }
        });
        textArea.hookIntoRuntime(atlasShellRuby.getRuby());
        atlasShellRuby.hookIntoGUIReadline(textArea);
        BiasedProcCompletor.hookIntoRuntime(atlasShellRuby.getRuby());
        atlasShellRuby.start();
    }

    public void shutdown() {
        assert SwingUtilities.isEventDispatchThread();
        textArea.shutdown();
    }

    @Override
    public void graphicalView(TableMetadata tableMetadata,
                     final String tableName,
                     final List<String> columns,
                     final List<RowResult<byte[]>> rows,
                     final boolean limitedResults) {
        assert SwingUtilities.isEventDispatchThread();
        final JTable table = new JTable();
        table.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
        table.setColumnSelectionAllowed(true);
        table.setRowSelectionAllowed(true);
        if (tableMetadata.getColumns().hasDynamicColumns()) {
            table.setModel(new AtlasShellDynamicColumnTableModel(tableMetadata, rows));
        } else {
            table.setModel(new AtlasShellNamedColumnTableModel(tableMetadata, columns, rows));
        }

        resizeTableColumnWidths(tableName, table);

        table.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
                if (e.getClickCount() == 2) {
                    showDetailPopup(table, e.getLocationOnScreen());
                }
            }
        });

        // Whenever the user changes the width of a column, save the column
        // width off. This way, if a user repeatedly issues queries against the
        // same table, he won't have to resize the columns each time.
        table.getColumnModel().addColumnModelListener(new TableColumnModelListener() {
            @Override
            public void columnMarginChanged(ChangeEvent e) {
                TableColumn column = table.getTableHeader().getResizingColumn();
                if (column != null) {
                    saveTableColumnWidth(tableName, column);
                }
            }

            @Override
            public void columnAdded(TableColumnModelEvent _) {
                //
            }

            @Override
            public void columnMoved(TableColumnModelEvent _) {
                //
            }

            @Override
            public void columnRemoved(TableColumnModelEvent _) {
                //
            }

            @Override
            public void columnSelectionChanged(ListSelectionEvent _) {
                //
            }
        });

        JScrollPane scrollPane = new JScrollPane(
                table,
                ScrollPaneConstants.VERTICAL_SCROLLBAR_AS_NEEDED,
                ScrollPaneConstants.HORIZONTAL_SCROLLBAR_AS_NEEDED);
        scrollPane.setAlignmentX(0);

        String message = String.format("%d %s", rows.size(), rows.size() == 1 ? "row" : "rows");
        if (limitedResults) {
            message = "Limited to " + message;
        }
        JLabel label = new JLabel(message);
        label.setBorder(new EmptyBorder(2, 2, 2, 2));
        label.setAlignmentX(0);

        JPanel panel = new JPanel();
        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));
        panel.add(scrollPane);
        panel.add(label);

        AtlasShellMainWindow.addCloseableTab(viewPane, tableName, panel, null);
    }

    @Override
    public boolean isGraphicalViewEnabled() {
        return true;
    }

    private final Map<String, Map<String, Integer>> tableNameToColumnNameToColumnWidth = Maps.newHashMap();

    private void saveTableColumnWidth(String tableName, TableColumn column) {
        Map<String, Integer> columnNameToColumnWidth = tableNameToColumnNameToColumnWidth.get(tableName);
        if (columnNameToColumnWidth == null) {
            columnNameToColumnWidth = Maps.newHashMap();
            tableNameToColumnNameToColumnWidth.put(tableName, columnNameToColumnWidth);
        }
        columnNameToColumnWidth.put(String.valueOf(column.getHeaderValue()), column.getWidth());
    }

    private void resizeTableColumnWidths(String tableName, JTable table) {
        Map<String, Integer> columnNameToColumnWidth = tableNameToColumnNameToColumnWidth.get(tableName);
        if (columnNameToColumnWidth == null) {
            columnNameToColumnWidth = ImmutableMap.of();
        }
        for (int i = 0; i < table.getColumnCount(); ++i) {
            String columnName = table.getColumnName(i);
            if (columnNameToColumnWidth.containsKey(columnName)) {
                table.getColumnModel().getColumn(i).setPreferredWidth(
                        columnNameToColumnWidth.get(columnName));
            } else {
                table.getColumnModel().getColumn(i).setPreferredWidth(200);
            }
        }
    }

    private void showDetailPopup(JTable table, Point location) {
        assert SwingUtilities.isEventDispatchThread();

        String value = String.valueOf(table.getValueAt(
                table.getSelectedRow(),
                table.getSelectedColumn()));

        if (AtlasShellRubyHelpers.isJsonObject(value)) {
            value = AtlasShellRubyHelpers.prettyPrintJson(value);
        }

        JTextArea detailTextArea = new JTextArea();
        detailTextArea.setMargin(new Insets(5, 5, 5, 5));
        detailTextArea.setEditable(false);
        detailTextArea.setOpaque(false);
        detailTextArea.setText(value);

        JScrollPane detailScrollPane = new JScrollPane();
        detailScrollPane.setViewportView(detailTextArea);

        final JFrame detailFrame = new JFrame();
        detailFrame.setTitle("Details");
        detailFrame.setSize(600, 400);
        detailFrame.setLocation(location);
        detailFrame.add(detailScrollPane, BorderLayout.CENTER);
        detailFrame.setVisible(true);

        ActionListener onEscPressed = new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                detailFrame.dispose();
            }
        };
        detailFrame.getRootPane().registerKeyboardAction(
                onEscPressed,
                KeyStroke.getKeyStroke(KeyEvent.VK_ESCAPE, 0),
                JComponent.WHEN_IN_FOCUSED_WINDOW);
    }

    @Override
    public void clear() {
        editorPane.setText("");

    }
}
