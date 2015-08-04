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
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.Map;

import javax.annotation.Nullable;
import javax.swing.Icon;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTabbedPane;
import javax.swing.SwingUtilities;

import com.google.common.collect.Maps;
import com.palantir.atlasdb.shell.audit.AuditLoggingConnection;

public class AtlasShellMainWindow implements AtlasShellToolBar.Actions {
    private static final Icon CLOSE_TAB_ICON = new ImageIcon(
            AtlasShellMainWindow.class.getResource("/closeTabButton.png"));

    private final AuditLoggingConnection auditLogger;
    private JTabbedPane tabbedPane;
    private Map<String, Integer> tabTitleCounts = Maps.newHashMap();

    private final AtlasShellContextFactory atlasShellContextFactory;

    public AtlasShellMainWindow(AuditLoggingConnection auditLogger, AtlasShellContextFactory atlasShellContextFactory) {
        assert SwingUtilities.isEventDispatchThread();

        JFrame frame = new JFrame("AtlasShell");
        tabbedPane = new JTabbedPane();
        frame.add(tabbedPane, BorderLayout.CENTER);
        AtlasShellToolBar atlasShellToolBar = new AtlasShellToolBar(this);
        frame.add(atlasShellToolBar.makeToolbar(), BorderLayout.PAGE_START);

        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setLocation(100, 100);
        frame.setSize(950, 800);
        frame.setVisible(true);

        this.auditLogger = auditLogger;
        this.atlasShellContextFactory = atlasShellContextFactory;
    }

    private void newShell(String tabTitle, AtlasShellRubyScriptlet connectScriptlet2) {
        assert SwingUtilities.isEventDispatchThread();
        final AtlasShellPanel panel = new AtlasShellPanel();
        panel.run(connectScriptlet2, auditLogger, atlasShellContextFactory);

        if (tabTitleCounts.containsKey(tabTitle)) {
            int currentCount = tabTitleCounts.get(tabTitle);
            tabTitleCounts.put(tabTitle, currentCount + 1);
            tabTitle = tabTitle + " (" + currentCount + ")";
        } else {
            tabTitleCounts.put(tabTitle, 1);
        }

        addCloseableTab(tabbedPane, tabTitle, panel, new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                assert SwingUtilities.isEventDispatchThread();
                panel.shutdown();
            }
        });
    }

    static void addCloseableTab(final JTabbedPane pane,
                                final String tabTitle,
                                final JComponent component,
                                final @Nullable ActionListener onClose) {
        assert SwingUtilities.isEventDispatchThread();

        JButton closeButton = new JButton(CLOSE_TAB_ICON);
        closeButton.setBorder(null);
        closeButton.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
                assert SwingUtilities.isEventDispatchThread();
                if (onClose != null) {
                    onClose.actionPerformed(null);
                }
                pane.remove(component);
            }
        });

        JPanel padding = new JPanel();
        padding.setOpaque(false);

        JPanel tabComponent = new JPanel();
        tabComponent.setOpaque(false);
        tabComponent.setLayout(new FlowLayout(FlowLayout.LEFT, 0, 0));
        tabComponent.add(new JLabel(tabTitle));
        tabComponent.add(padding);
        tabComponent.add(closeButton);

        pane.add(component);
        pane.setSelectedIndex(pane.getTabCount() - 1);
        pane.setTabComponentAt(pane.getTabCount() - 1, tabComponent);
    }

    @Override
    public void doConnect() {
        AtlasShellConnectionDialogModel model = new AtlasShellConnectionDialogModel();
        AtlasShellConnectionDialogView.create(model);

        if (model.shouldConnect()) {
            String rawScriptlet = String.format(
                    "connect(:type=>'%s',:host=>'%s',:port=>'%s',:identifier=>'%s',:username=>'%s',:password=>'@PASSWORD@')",
                    model.getTypeText(),
                    model.getHostText(),
                    model.getPortText(),
                    model.getIdentifierText(),
                    model.getUsernameText());
            AtlasShellRubyScriptlet atlasShellRubyScriptlet = new AtlasShellRubyScriptlet(
                    rawScriptlet);
            atlasShellRubyScriptlet.substitute("@PASSWORD@", String.valueOf(model.getPasswordText()));
            newShell(model.getConnectionNameText(), atlasShellRubyScriptlet);
        }
    }

    @Override
    public void doDispatchPrefsShell() {
        newShell("dispatch.prefs", new AtlasShellRubyScriptlet("connect_with_dispatch_prefs()"));
    }

    @Override
    public void doNewShell() {
        AtlasShellRubyScriptlet atlasShellRubyScriptlet = new AtlasShellRubyScriptlet("");
        newShell("shell", atlasShellRubyScriptlet);
    }

    @Override
    public void doHelp() {
        new AtlasShellHelp().show();
    }
}
