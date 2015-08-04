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
import java.awt.Component;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;

import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTextField;
import javax.swing.KeyStroke;
import javax.swing.border.EmptyBorder;
import javax.swing.border.EtchedBorder;

import com.palantir.ptoss.cinch.core.Bindable;
import com.palantir.ptoss.cinch.core.Bindings;
import com.palantir.ptoss.cinch.core.CallOnUpdate;
import com.palantir.ptoss.cinch.swing.Action;
import com.palantir.ptoss.cinch.swing.Bound;
import com.palantir.ptoss.cinch.swing.BoundSelection;

@Bindable
public class AtlasShellConnectionDialogView {
    private static final int DEFAULT_COL_SIZE = 20;
    private final Bindings bindings = new Bindings();
    private final JDialog dialog = new JDialog();

    private final AtlasShellConnectionDialogModel model;

    @Bindable
    private final AtlasShellConnectionDialogController controller;

    @Bound(to = "savedDbConnections")
    @BoundSelection(to = "selectedItem")
    private final JList connectionList = new JList();

    @Action(call = "connectAction")
    private final JButton connectButton = new JButton("Connect");
    @Action(call = "saveAction")
    private final JButton saveButton = new JButton("Save");
    @Action(call = "deleteAction")
    private final JButton deleteButton = new JButton("Delete");

    @Bound(to = "connectionNameText")
    private final JTextField connectionNameText = createJTextField();
    @Bound(to = "hostText")
    private final JTextField hostText = createJTextField();
    @Bound(to = "portText")
    private final JTextField portText = createJTextField();
    @Bound(to = "typeList")
    @BoundSelection(to = "typeText")
    @Action(call = "typeChanged")
    private final JComboBox typeText = createJComboBox();

    public void typeChanged() {
        AtlasShellConnectionType connectionType =
            AtlasShellConnectionType.valueOf((String) typeText.getSelectedItem());
        if (connectionType.isIdentifierRequired()) {
            identifierText.setText(connectionType.getDefaultIdentifier());
            identifierText.setVisible(true);
            identifierLabel.setVisible(true);
            identifierLabel.setText(connectionType.getIdentifierName() + ":");
        } else {
            identifierText.setText(null);
            identifierText.setVisible(false);
            identifierLabel.setVisible(false);
        }
        if (connectionType == AtlasShellConnectionType.MEMORY) {
            hostText.setText(null);
            hostText.setVisible(false);
            hostLabel.setVisible(false);

            portText.setText(null);
            portText.setVisible(false);
            portLabel.setVisible(false);
        } else {
            hostText.setText(connectionType.getDefaultHostname());
            hostText.setVisible(true);
            hostLabel.setVisible(true);

            portText.setText(connectionType.getDefaultPort());
            portLabel.setVisible(true);
            portText.setVisible(true);
        }
    }

    @Bound(to = "identifierText")
    private final JTextField identifierText = createJTextField();
    @Bound(to = "usernameText")
    private final JTextField usernameText = createJTextField();
    @Bound(to = "passwordText")
    @Action(call = "connectAction")
    private final JPasswordField passwordText = createJPasswordField();

    private final JLabel identifierLabel = new JLabel("Identifier:");
    private final JLabel hostLabel = new JLabel("Host:");
    private final JLabel portLabel = new JLabel("Port:");
    private final JLabel usernameLabel = new JLabel("Username:");
    private final JLabel passwordLabel = new JLabel("Password (not saved):");

    private AtlasShellConnectionDialogView(AtlasShellConnectionDialogModel model) {
        this.model = model;
        this.controller = new AtlasShellConnectionDialogController(model);
    }

    public static AtlasShellConnectionDialogView create(AtlasShellConnectionDialogModel model) {
        AtlasShellConnectionDialogView popup = new AtlasShellConnectionDialogView(model);
        popup.bindings.bind(popup);
        popup.showUi();
        return popup;
    }

    private JPasswordField createJPasswordField() {
        JPasswordField passwordField = new JPasswordField(DEFAULT_COL_SIZE);
        passwordField.setMaximumSize(new Dimension(
                Integer.MAX_VALUE,
                passwordField.getPreferredSize().height));
        return passwordField;
    }

    private JTextField createJTextField() {
        JTextField textField = new JTextField(DEFAULT_COL_SIZE);
        textField.setMaximumSize(new Dimension(
                Integer.MAX_VALUE,
                textField.getPreferredSize().height));
        return textField;
    }

    private JComboBox createJComboBox() {
        JComboBox comboBox = new JComboBox();
        comboBox.setMaximumSize(new Dimension(
                Integer.MAX_VALUE,
                DEFAULT_COL_SIZE));
        return comboBox;
    }

    private static JPanel panelOf(JComponent... comps) {
        JPanel p = new JPanel();
        p.setOpaque(false);
        for (JComponent comp : comps) {
            comp.setAlignmentX(Component.LEFT_ALIGNMENT);
            p.add(comp);
        }
        return p;
    }

    @CallOnUpdate
    private void disposeDialogIfNecessary() {
        if (model.shouldConnect()) {
            dialog.dispose();
        }
    }

    private void showUi() {
        ActionListener onEscPressed = new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                dialog.dispose();
            }
        };
        dialog.getRootPane().registerKeyboardAction(
                onEscPressed,
                KeyStroke.getKeyStroke(KeyEvent.VK_ESCAPE, 0),
                JComponent.WHEN_IN_FOCUSED_WINDOW);

        dialog.setTitle("Select/Create Connection");
        dialog.setModal(true);
        dialog.setLocation(100, 100);
        dialog.setSize(300, 600);

        connectionList.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
                if (e.getClickCount() == 2) {
                    controller.connectAction();
                }
            }
        });

        JScrollPane scrollPane = new JScrollPane();
        scrollPane.setViewportView(connectionList);
        scrollPane.setBorder(new EtchedBorder());

        JPanel connectionPanel = new JPanel();
        connectionPanel.setOpaque(false);
        connectionPanel.setLayout(new BorderLayout());
        connectionPanel.add(new JLabel("Saved Connections:"), BorderLayout.NORTH);
        connectionPanel.add(scrollPane, BorderLayout.CENTER);
        connectionPanel.setBorder(new EmptyBorder(10, 10, 10, 5));

        JPanel buttonPanel = panelOf(connectButton, new JPanel(), // padding
                saveButton,
                new JPanel(), // padding
                deleteButton);
        buttonPanel.setLayout(new FlowLayout(FlowLayout.LEFT, 0, 0));

        JPanel createConnPanel = panelOf(
                new JLabel("Connection Name:"),
                connectionNameText,
                new JLabel("Type:"),
                typeText,
                hostLabel,
                hostText,
                portLabel,
                portText,
                identifierLabel,
                identifierText,
                buttonPanel);
        createConnPanel.setLayout(new BoxLayout(createConnPanel, BoxLayout.Y_AXIS));
        createConnPanel.setBorder(new EmptyBorder(10, 10, 10, 10));

        JSplitPane splitPane = new JSplitPane(
                JSplitPane.HORIZONTAL_SPLIT,
                connectionPanel,
                createConnPanel);

        dialog.setContentPane(splitPane);
        dialog.pack();
        dialog.setVisible(true);
    }
}
