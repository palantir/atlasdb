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

import javax.swing.JOptionPane;

public class AtlasShellConnectionDialogController {
    private final AtlasShellConnectionDialogModel model;

    public AtlasShellConnectionDialogController(AtlasShellConnectionDialogModel model) {
        this.model = model;
    }

    public void connectAction() {
        if (!model.validateCurrentConnectionForConnect()) {
            JOptionPane.showMessageDialog(null, "Cannot connect as some fields are incomplete.");
            return;
        }
        model.setShouldConnect();
    }

    public void saveAction() {
        if (model.getSavedDbConnections().contains(model.getConnectionNameText())
                && !confirmOverwrite()) {
            return;
        }
        if (!model.validateCurrentConnectionForSave()) {
            JOptionPane.showMessageDialog(
                    null,
                    "Cannot save this connection as some fields are incomplete.");
            return;
        }
        model.saveCurrentConnection();
    }

    private boolean confirmOverwrite() {
        return JOptionPane.showConfirmDialog(
                null,
                "A connection with this name already exists.\nWould you like to overwrite it?",
                "Confirm Overwrite",
                JOptionPane.YES_NO_OPTION) == JOptionPane.YES_OPTION;
    }

    public void deleteAction() {
        model.deleteCurrentConnection();
    }
}
