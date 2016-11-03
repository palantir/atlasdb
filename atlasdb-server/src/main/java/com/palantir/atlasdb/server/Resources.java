/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.server;

import org.glassfish.jersey.server.model.Resource;

public final class Resources {
    private Resources() {
        // Utility class
    }

    public static Resource getInstancedResourceAtPath(String path, Object instance) {
        Resource baseResource = Resource.from(instance.getClass());
        Resource resourceWithInstance = addInstanceToResource(baseResource, instance);
        return setPathOnResource(resourceWithInstance, path);
    }

    public static Resource setPathOnResource(Resource resource, String path) {
        return Resource.builder(resource)
                .path(path)
                .build();
    }

    public static Resource addInstanceToResource(Resource resource, Object handlerInstance) {
        Resource.Builder newResource = Resource.builder()
                .name(resource.getName())
                .path(resource.getPath())
                .extended(resource.isExtended());

        resource.getChildResources().forEach(child ->
                newResource.addChildResource(addInstanceToResource(child, handlerInstance)));

        resource.getResourceMethods().forEach(method ->
                newResource.addMethod(method)
                        .handledBy(handlerInstance, method.getInvocable().getDefinitionMethod()));

        if (resource.getResourceLocator() != null) {
            newResource.addMethod(resource.getResourceLocator())
                    .handledBy(handlerInstance, resource.getResourceLocator().getInvocable().getDefinitionMethod());
        }

        return newResource.build();
    }
}
