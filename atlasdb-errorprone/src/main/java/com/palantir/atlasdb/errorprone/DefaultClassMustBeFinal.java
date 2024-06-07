/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.errorprone;

import com.google.auto.service.AutoService;
import com.google.errorprone.BugPattern;
import com.google.errorprone.BugPattern.SeverityLevel;
import com.google.errorprone.VisitorState;
import com.google.errorprone.bugpatterns.BugChecker;
import com.google.errorprone.fixes.SuggestedFixes;
import com.google.errorprone.matchers.Description;
import com.sun.source.tree.ClassTree;
import com.sun.source.tree.Tree.Kind;
import javax.lang.model.element.Modifier;

@AutoService(BugChecker.class)
@BugPattern(
        severity = SeverityLevel.WARNING,
        name = "",
        summary = "Default implementation classes must be marked as final")
public final class DefaultClassMustBeFinal extends BugChecker implements BugChecker.ClassTreeMatcher {
    @Override
    public Description matchClass(ClassTree tree, VisitorState state) {
        if (tree.getKind().equals(Kind.CLASS)
                && !tree.getModifiers().getFlags().contains(Modifier.ABSTRACT)
                && tree.getSimpleName().toString().startsWith("Default")
                && !tree.getModifiers().getFlags().contains(Modifier.FINAL)) {
            return buildDescription(tree)
                    .setMessage("Default implementation classes must be marked as final")
                    .addFix(SuggestedFixes.addModifiers(tree, state, Modifier.FINAL))
                    .build();
        }
        return Description.NO_MATCH;
    }
}
