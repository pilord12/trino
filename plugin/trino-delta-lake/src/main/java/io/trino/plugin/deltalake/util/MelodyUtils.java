package io.trino.plugin.deltalake.util;

public class MelodyUtils {
    public static String getOrgFromSchema(String schema) {
        return schema.split("/")[0];
    }

    public static String getDomainFromSchema(String schema) {
        return schema.split("/")[1];
    }
}
