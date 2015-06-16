// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that although a method or field on a @PublicApi class is visible,
 * it is not intended for use by non-Palantir code.
 *
 * All @PrivateApi occurrences will eventually be phased out.
 *
 * @author jgarrod
 *
 */
@Retention(RetentionPolicy.RUNTIME) // We need runtime for static analasys.
@Target({ ElementType.TYPE,
          ElementType.ANNOTATION_TYPE,
          ElementType.METHOD,
          ElementType.CONSTRUCTOR,
          ElementType.PACKAGE,
          ElementType.FIELD
          })
@PublicApi // not part of the api, but must be in the api jar
public @interface PrivateApi {
    // marker annotation
}
