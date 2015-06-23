package com.palantir.atlasdb.shell;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.palantir.nexus.db.DbConnectionInfo;
import com.palantir.ptoss.cinch.core.DefaultBindableModel;
import com.palantir.ptoss.util.Throwables;

public class AtlasShellConnectionDialogModel extends DefaultBindableModel {
    private List<String> savedDbConnections = Lists.newArrayList();
    private Map<String, DbConnectionInfo> savedDbConnectionInfos = Maps.newHashMap();

    private String connectionNameText = "";
    private String hostText = "";
    private String portText = "";
    private String typeText = "";
    private String identifierText = "";
    private String usernameText = "";
    private char[] passwordText = new char[0];
    private String selectedItem;
    private boolean shouldConnect = false;

    public AtlasShellConnectionDialogModel() {
        savedDbConnectionInfos.putAll(readConnections());
        savedDbConnections.addAll(savedDbConnectionInfos.keySet());
        Collections.sort(savedDbConnections);
    }

    public String getConnectionNameText() {
        return connectionNameText;
    }

    public void setConnectionNameText(String connectionNameText) {
        this.connectionNameText = connectionNameText;
        this.update();
    }

    public String getHostText() {
        return hostText;
    }

    public void setHostText(String hostText) {
        this.hostText = hostText;
        this.update();
    }

    public String getPortText() {
        return portText;
    }

    public void setPortText(String portText) {
        this.portText = portText;
        this.update();
    }

    public List<String> getTypeList() {
        return AtlasShellConnectionType.getTypeList();
    }

    public String getTypeText() {
        return typeText;
    }

    public void setTypeText(String typeText) {
        this.typeText = typeText;
        this.update();
    }

    public String getIdentifierText() {
        return identifierText;
    }

    public void setIdentifierText(String identifierText) {
        this.identifierText = identifierText;
        this.update();
    }

    public String getUsernameText() {
        return usernameText;
    }

    public void setUsernameText(String usernameText) {
        this.usernameText = usernameText;
        this.update();
    }

    public char[] getPasswordText() {
        return passwordText;
    }

    public void setPasswordText(char[] passwordText) {
        this.passwordText = passwordText;
        this.update();
    }

    public void setSavedDbConnections(List<String> savedDbConnections) {
        this.savedDbConnections = savedDbConnections;
        this.setSelectedItem(Iterables.getFirst(this.savedDbConnections, null));
        this.update();
    }

    public List<String> getSavedDbConnections() {
        return savedDbConnections;
    }

    public void setSelectedItem(String selectedItem) {
        this.selectedItem = selectedItem;
        if (selectedItem == null) {
            clearAllFieldsExceptType();
            return;
        }
        DbConnectionInfo info = savedDbConnectionInfos.get(selectedItem);
        connectionNameText = selectedItem;
        typeText = info.getType();
        hostText = info.getHost();
        portText = info.getPort();
        identifierText = info.getIdentifier();
        usernameText = info.getUsername();
        passwordText = info.getPassword().toCharArray();
        update();
    }

    public String getSelectedItem() {
        return selectedItem;
    }

    public void appendSavedDbConnections(String connectionName) {
        savedDbConnections.add(connectionName);
        setSelectedItem(connectionName);
        this.update();
    }

    public void removeSavedDbConnections(String nameToRemove) {
        savedDbConnections.remove(nameToRemove);
        if (Objects.equal(selectedItem, nameToRemove)) {
            setSelectedItem(Iterables.getFirst(savedDbConnections, null));
        }
        this.update();
    }

    public void saveCurrentConnection() {
        DbConnectionInfo info = DbConnectionInfo.create(
                hostText,
                portText,
                identifierText,
                typeText,
                usernameText,
                "");
        if (!savedDbConnectionInfos.containsKey(connectionNameText)) {
            savedDbConnections.add(connectionNameText);
        }
        savedDbConnectionInfos.put(connectionNameText, info);
        writeConnections(savedDbConnectionInfos);
        update();
    }

    public void deleteCurrentConnection() {
        if (!savedDbConnections.contains(connectionNameText)) {
            return;
        }
        savedDbConnections.remove(connectionNameText);
        savedDbConnectionInfos.remove(connectionNameText);
        writeConnections(savedDbConnectionInfos);
        update();
    }

    public boolean validateCurrentConnectionForConnect() {
        return validateCurrentConnectionForSave() && passwordText.length > 0;
    }

    public boolean validateCurrentConnectionForSave() {
        return !connectionNameText.isEmpty() && !hostText.isEmpty() && !portText.isEmpty()
                && !typeText.isEmpty() && validateIdentifier() && !usernameText.isEmpty();
    }

    private boolean validateIdentifier() {
        if (typeText.isEmpty()) {
            return false;
        }
        boolean identifierRequired = AtlasShellConnectionType.valueOf(typeText).isIdentifierRequired();
        return !(identifierRequired && identifierText.isEmpty());
    }

    public boolean shouldConnect() {
        return shouldConnect;
    }

    public void setShouldConnect() {
        shouldConnect = true;
        update();
    }

    // Note: we don't clear the 'type' field because it's a dropdown, as
    // opposed to a text field
    private void clearAllFieldsExceptType() {
        connectionNameText = "";
        hostText = "";
        portText = "";
        identifierText = "";
        usernameText = "";
        passwordText = new char[0];
        this.update();
    }

    private static final File FILE = new File("atlasdb_shell_saved_connections");

    @SuppressWarnings("unchecked")
    public static Map<String, DbConnectionInfo> readConnections() {
        if (!FILE.exists()) {
            return ImmutableMap.of();
        }
        try {
            Gson gson = new Gson();
            Type collectionType = new TypeToken<Map<String, DbConnectionInfo>>() { /* empty */
            }.getType();
            return (Map<String, DbConnectionInfo>) gson.fromJson(
                    Files.toString(FILE, Charsets.UTF_8),
                    collectionType);
        } catch (IOException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    public static void writeConnections(Map<String, DbConnectionInfo> connections) {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        try {
            Files.write(gson.toJson(connections), FILE, Charsets.UTF_8);
        } catch (IOException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }
}
