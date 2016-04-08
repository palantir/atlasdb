package com.palantir.util;

import java.util.Iterator;

import com.google.common.base.Splitter;

public class VersionStrings {

    public static int compareVersions(String v1, String v2) {
        Iterator<String> v1Iter = Splitter.on('.').trimResults().split(v1).iterator();
        Iterator<String> v2Iter = Splitter.on('.').trimResults().split(v2).iterator();
        while (v1Iter.hasNext() && v2Iter.hasNext()) {
            int v1Part = Integer.parseInt(v1Iter.next());
            int v2Part = Integer.parseInt(v2Iter.next());
            if (v1Part != v2Part) {
                return v1Part - v2Part;
            }
        }
        while (v1Iter.hasNext()) {
            if (Integer.parseInt(v1Iter.next()) != 0) {
                return 1;
            }
        }
        while (v2Iter.hasNext()) {
            if (Integer.parseInt(v2Iter.next()) != 0) {
                return -1;
            }
        }
        return 0;
    }
}
