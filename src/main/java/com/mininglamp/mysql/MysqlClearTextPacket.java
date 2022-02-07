package com.mininglamp.mysql;


import java.nio.ByteBuffer;

public class MysqlClearTextPacket extends MysqlPacket {

    private String password = "";

    public String getPassword() {
        return password;
    }

    @Override
    public boolean readFrom(ByteBuffer buffer) {
        password = new String(MysqlProto.readNulTerminateString(buffer));
        return true;
    }

    @Override
    public void writeTo(MysqlSerializer serializer) {

    }
}
