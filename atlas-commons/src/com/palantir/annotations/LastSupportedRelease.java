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

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.CONSTRUCTOR;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.LOCAL_VARIABLE;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PACKAGE;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * Indicates the last release in which the specified element will be
 * officially supported.  Subsequent releases may break compile time
 * compatibility.  Subsequent releases that change the semantic
 * meaning of the annotated element but which are compile time
 * compatible will have their semantic changes thoroughly documented
 * in the release notes.
 *
 * As a matter of style, the annotated element should be commented to
 * indicate an alternative way of getting the same functionality which
 * will be supported longer, and the annotated element should be
 * deprecated.
 *
 * @author jgarrod
 *
 */
@Target({
    TYPE,
    FIELD,
    METHOD,
    PARAMETER,
    CONSTRUCTOR,
    LOCAL_VARIABLE,
    ANNOTATION_TYPE,
    PACKAGE})
@Retention(RetentionPolicy.RUNTIME)
@PublicApi
public @interface LastSupportedRelease {
    /**
     * The external version, e.g. "2.2"
     */
    String release();
}

