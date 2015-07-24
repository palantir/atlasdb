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
