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
package com.palantir.lock.logger;

import com.palantir.lock.LockDescriptor;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;

/**
 * A simple wrapper for writing lock state as YAML to a file.
 */
class LockStateYamlWriter implements Closeable {
    private static final Yaml yaml = new Yaml(getRepresenter(), getDumperOptions());

    private final Writer writer;

    LockStateYamlWriter(Writer fileWriter) {
        this.writer = fileWriter;
    }

    /**
     * Creates a LockStateYamlWriter for the given file in append mode.
     */
    public static LockStateYamlWriter create(File file) throws IOException {
        return new LockStateYamlWriter(Files.newBufferedWriter(
                file.toPath(), StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND));
    }

    /**
     * Write an object to the file as YAML.
     */
    public void dumpObject(Object data) {
        yaml.dump(data, writer);
    }

    /**
     * Write a string to the file as a YAML comment.
     * The string must be a single line.
     */
    public void appendComment(String string) throws IOException {
        writer.append("# ");
        writer.append(string);
        writer.append("\n");
    }

    private static Representer getRepresenter() {
        Representer representer = new LockDescriptorAwareRepresenter();
        representer.addClassTag(ImmutableSimpleTokenInfo.class, Tag.MAP);
        representer.addClassTag(ImmutableSimpleLockRequest.class, Tag.MAP);
        representer.addClassTag(ImmutableSanitizedLockRequestProgress.class, Tag.MAP);
        representer.addClassTag(SimpleLockRequestsWithSameDescriptor.class, Tag.MAP);
        representer.addClassTag(LockDescriptor.class, Tag.MAP);
        return representer;
    }

    private static DumperOptions getDumperOptions() {
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        options.setIndent(4);
        options.setAllowReadOnlyProperties(true);
        options.setExplicitStart(true);
        return options;
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }

    private static class LockDescriptorAwareRepresenter extends Representer {
        LockDescriptorAwareRepresenter() {
            super();
            this.representers.put(LockDescriptor.class, data -> representScalar(Tag.STR, data.toString()));
        }
    }
}
