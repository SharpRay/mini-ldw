package org.rzlabs.mysql;

public class MysqlAuthSwitchPacket extends MysqlPacket {
    private static final int STATUS = 0xfe;
    private static final String AUTH_PLUGIN_NAME = "mysql_clear_password";
    private static final String DATA = "";

    @Override
    public void writeTo(MysqlSerializer serializer) {
        serializer.writeInt1(STATUS);
        serializer.writeNulTerminateString(AUTH_PLUGIN_NAME);
        serializer.writeNulTerminateString(DATA);
    }
}
