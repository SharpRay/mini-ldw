package org.rzlabs.mysql;


import com.google.common.collect.Maps;
import org.rzlabs.common.Config;

import java.nio.ByteBuffer;
import java.util.Map;

// MySQL protocol handshake response packet, which contain authenticate information.
public class MysqlAuthPacket extends MysqlPacket {
    private int maxPacketSize;
    private int characterSet;
    private String userName;
    private byte[] authResponse;
    private String database;
    private String pluginName;
    private MysqlCapability capability;
    private Map<String, String> connectAttributes;
    private byte[] randomString;

    public String getUser() {
        return userName;
    }

    public byte[] getAuthResponse() {
        return authResponse;
    }

    public void setAuthResponse(byte[] bytes) {
        authResponse = bytes;
    }

    public String getDb() {
        return database;
    }

    public byte[] getRandomString() {
        return randomString;
    }

    public MysqlCapability getCapability() {
        return capability;
    }

    public String getPluginName() {
        return pluginName;
    }

    @Override
    public boolean readFrom(ByteBuffer buffer) {
        // read capability four byte, which CLIENT_PROTOCOL_41 must be set
        capability = new MysqlCapability(MysqlProto.readInt4(buffer));
        if (!capability.isProtocol41()) {
            return false;
        }
        // max packet size
        maxPacketSize = MysqlProto.readInt4(buffer);
        // character set. only support 33(utf-8)?
        characterSet = MysqlProto.readInt1(buffer);
        // reserved 23 bytes
        if (new String(MysqlProto.readFixedString(buffer, 3)).equals(Config.proxy_auth_magic_prefix)) {
            randomString = new byte[MysqlPassword.SCRAMBLE_LENGTH];
            buffer.get(randomString);
        } else {
            buffer.position(buffer.position() + 20);
        }
        // user name
        userName = new String(MysqlProto.readNulTerminateString(buffer));
        if (capability.isPluginAuthDataLengthEncoded()) {
            authResponse = MysqlProto.readLenEncodedString(buffer);
        } else if (capability.isSecureConnection()) {
            int len = MysqlProto.readInt1(buffer);
            authResponse = MysqlProto.readFixedString(buffer, len);
        } else {
            authResponse = MysqlProto.readNulTerminateString(buffer);
        }
        // maybe no data anymore
        // DB to use
        if (buffer.remaining() > 0 && capability.isConnectedWithDb()) {
            database = new String(MysqlProto.readNulTerminateString(buffer));
        }
        // plugin name to plugin
        if (buffer.remaining() > 0 && capability.isPluginAuth()) {
            pluginName = new String(MysqlProto.readNulTerminateString(buffer));
        }
        // attribute map, no use now.
        if (buffer.remaining() > 0 && capability.isConnectAttrs()) {
            connectAttributes = Maps.newHashMap();
            long numPair = MysqlProto.readVInt(buffer);
            for (long i = 0; i < numPair; ++i) {
                String key = new String(MysqlProto.readLenEncodedString(buffer));
                String value = new String(MysqlProto.readLenEncodedString(buffer));
                connectAttributes.put(key, value);
            }
        }

        // Commented for JDBC
        // if (buffer.remaining() != 0) {
        //     return false;
        // }
        return true;
    }

    @Override
    public void writeTo(MysqlSerializer serializer) {

    }
}
