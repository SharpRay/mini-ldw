package com.mininglamp.common.util;

import com.google.common.base.Strings;

public class SqlUtils {
    public static String escapeUnquote(String ident) {
        return ident.replaceAll("``", "`");
    }

    public static String getIdentSql(String ident) {
        StringBuilder sb = new StringBuilder();
        sb.append('`');
        for (char ch : ident.toCharArray()) {
            if (ch == '`') {
                sb.append("``");
            } else {
                sb.append(ch);
            }
        }
        sb.append('`');
        return sb.toString();
    }

    public static String escapeQuota(String str) {
        if (Strings.isNullOrEmpty(str)) {
            return str;
        }
        return str.replaceAll("\"", "\\\\\"");
    }
}
