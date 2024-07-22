package org.apache.gluten.execution;

import java.util.Set;

public class CHNativeCacheManager {
    public static void cacheParts(String table, Set<String> columns, boolean async) {
        nativeCacheParts(table, String.join(",", columns), async);
    }

    private static native void nativeCacheParts(String table, String columns, boolean async);
}
