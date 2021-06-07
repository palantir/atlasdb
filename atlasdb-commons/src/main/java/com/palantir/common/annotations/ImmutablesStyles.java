/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.common.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.immutables.value.Value;

public interface ImmutablesStyles {
    @Target({ElementType.PACKAGE, ElementType.TYPE})
    @Retention(RetentionPolicy.SOURCE)
    @Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
    @interface PackageVisibleImmutablesStyle {}

    @Target({ElementType.PACKAGE, ElementType.TYPE})
    @Retention(RetentionPolicy.SOURCE)
    @Value.Style(stagedBuilder = true)
    @interface StagedBuilderStyle {}

    @Target({ElementType.PACKAGE, ElementType.TYPE})
    @Retention(RetentionPolicy.SOURCE)
    @Value.Style(allParameters = true)
    @interface AllParametersStyle {}

    @Target({ElementType.PACKAGE, ElementType.TYPE})
    @Retention(RetentionPolicy.SOURCE)
    @Value.Style(attributeBuilderDetection = true)
    @interface AttributeBuilderDetectionStyle {}
}
