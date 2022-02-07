package com.mininglamp.mysql;

import java.nio.ByteBuffer;

public abstract class MysqlPacket {
    public boolean readFrom(ByteBuffer deserializer) {
        // Only used to read authenticate packet from client.
        return false;
    }

    abstract public void writeTo(MysqlSerializer serializer);
}
