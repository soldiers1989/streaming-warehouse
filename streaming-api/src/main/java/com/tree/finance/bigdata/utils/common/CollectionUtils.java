package com.tree.finance.bigdata.utils.common;

import java.util.Collection;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/9/4 15:37
 */
public class CollectionUtils {
    public static String toString(Collection c, String sep) {
        if (c.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (Object item : c) {
            sb.append(item).append(sep);
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    public static boolean isEmpty(Collection c) {
        return c == null || c.isEmpty();
    }
}
