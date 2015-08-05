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

import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.util.Map;
import java.util.Map.Entry;

import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JPanel;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
public class AtlasShellToolBar {
    public AtlasShellToolBar(Actions toolBarActions) {
        this.toolBarActions = toolBarActions;
    }

    public interface Actions {
        void doConnect();

        void doNewShell();

        void doDispatchPrefsShell();

        void doHelp();
    }

    public JComponent makeToolbar() {
        JPanel toolBar = new JPanel(new GridLayout(1, 0));
        for (Entry<String, ActionListener> labelAndActionListener : getLabelsToActions().entrySet()) {
            String label = labelAndActionListener.getKey();
            ActionListener actionListener = labelAndActionListener.getValue();
            JButton button = new JButton(label);
            button.addActionListener(actionListener);
            button.setMargin(new Insets(5, 10, 5, 10));
            toolBar.add(button);
        }
        JPanel container = new JPanel(new FlowLayout(FlowLayout.LEFT));
        container.add(toolBar);
        return container;
    }

    private final Actions toolBarActions;

    private Map<String, ActionListener> getLabelsToActions() {
        Builder<String, ActionListener> builder = ImmutableMap.<String, ActionListener> builder();
        builder.put("New Connected Shell", new ActionListener() {
                    @Override
                    public void actionPerformed(ActionEvent arg0) {
                        toolBarActions.doConnect();
                    }});
        File file = new File("dispatch.prefs");
        if (file.exists() && file.canRead()) {
            builder.put("Connect using dispatch.prefs", new ActionListener() {
                        @Override
                        public void actionPerformed(ActionEvent arg0) {
                            toolBarActions.doDispatchPrefsShell();
                        }});
        }
        builder.put("New Unconnected Shell", new ActionListener() {
                    @Override
                    public void actionPerformed(ActionEvent arg0) {
                        toolBarActions.doNewShell();
                    }});
        builder.put("Help", new ActionListener() {
                    @Override
                    public void actionPerformed(ActionEvent arg0) {
                        toolBarActions.doHelp();
                    }
                });
        return builder.build();
    }
}
