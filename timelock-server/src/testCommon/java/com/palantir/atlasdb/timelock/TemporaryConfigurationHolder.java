/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.timelock;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;
import java.io.File;
import java.io.FileWriter;
import java.util.Locale;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

public class TemporaryConfigurationHolder extends ExternalResource {

    private static final Configuration TEMPLATE_CONFIG = templateConfig();

    private final TemporaryFolder temporaryFolder;
    private final String templateName;
    private final ImmutableTemplateVariables variables;

    private File temporaryConfigFile;

    TemporaryConfigurationHolder(
            TemporaryFolder temporaryFolder,
            String templateName,
            TemplateVariables variables) {
        this.temporaryFolder = temporaryFolder;
        this.templateName = templateName;
        this.variables = ImmutableTemplateVariables.copyOf(variables);
    }

    private static Configuration templateConfig() {
        Configuration config = new Configuration(Configuration.VERSION_2_3_29);
        config.setClassLoaderForTemplateLoading(TemporaryConfigurationHolder.class.getClassLoader(), "/");
        config.setDefaultEncoding("UTF-8");
        config.setLocale(Locale.UK);
        config.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
        return config;
    }

    @Override
    public void before() throws Exception {
        temporaryConfigFile = temporaryFolder.newFile();
        createTemporaryConfigFile();
    }

    private void createTemporaryConfigFile() throws Exception {
        Template template = TEMPLATE_CONFIG.getTemplate(templateName);
        TemplateVariables variablesWithFolders = variables
                .withDataDirectory(temporaryFolder.newFolder(appendPort("legacy")).getAbsolutePath())
                .withSqliteDataDirectory(temporaryFolder.newFolder(appendPort("sqlite")).getAbsolutePath());
        template.process(variablesWithFolders, new FileWriter(temporaryConfigFile));
    }

    private String appendPort(String legacy) {
        return legacy + variables.getLocalServerPort();
    }

    String getTemporaryConfigFileLocation() {
        return temporaryConfigFile.getPath();
    }

}
