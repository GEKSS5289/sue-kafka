package com.sue.kafka.api.serializer;

import com.sue.kafka.api.entity.User;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author sue
 * @date 2020/8/17 15:25
 */

public class UserDeSerializer implements Deserializer<User> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public User deserialize(String s, byte[] bytes) {
        if(bytes == null){
            return null;
        }
        if(bytes.length < 8){
            throw new SerializationException("size is wrong ,must be data length>=8");
        }
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        int idLen = byteBuffer.getInt();
        byte[] idByte = new byte[idLen];
        byteBuffer.get(idByte);

        int nameLen = byteBuffer.getInt();
        byte[] nameByte = new byte[nameLen];
        byteBuffer.get(nameByte);

        String id;
        String name;
        try {
            id = new String(idByte,"UTF-8");
            name = new String(nameByte,"UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("deserializing error!",e);
        }

        return new User(id,name);
    }

    @Override
    public void close() {

    }
}
