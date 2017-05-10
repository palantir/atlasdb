/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.lock.logger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;

import com.palantir.lock.LockDescriptor;

/**
 * Created by davidt on 4/20/17.
 */
class YamlWriter {
    private static final Yaml yaml = new Yaml(getRepresenter(), getDumperOptions());

    private final FileWriter fileWriter;

    YamlWriter(FileWriter fileWriter) {
        this.fileWriter = fileWriter;
    }

    public static YamlWriter create(File file) throws IOException {
        return new YamlWriter(new FileWriter(file));
    }

    public void writeToYaml(Object data) {
        yaml.dump(data, fileWriter);
    }

    public void appendString(String string) throws IOException {
        fileWriter.append(string);
    }

    private static Representer getRepresenter() {
        Representer representer = new LockDescriptorAwareRepresenter();
        representer.addClassTag(ImmutableSimpleTokenInfo.class, Tag.MAP);
        representer.addClassTag(ImmutableSimpleLockRequest.class, Tag.MAP);
        representer.addClassTag(SimpleLockRequestsWithSameDescriptor.class, Tag.MAP);
        representer.addClassTag(LockDescriptor.class, Tag.MAP);
        return representer;
    }

    private static DumperOptions getDumperOptions() {
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        options.setIndent(4);
        options.setAllowReadOnlyProperties(true);
        return options;
    }

    private static class LockDescriptorAwareRepresenter extends Representer {
        LockDescriptorAwareRepresenter() {
            this.representers.put(LockDescriptor.class, data -> representScalar(Tag.STR, data.toString()));
        }
    }
}
