package com.sue.kafka.api.serializer;

import com.sue.kafka.api.entity.User;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author sue
 * @date 2020/8/17 15:25
 */

public class UserSerializer implements Serializer<User> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }

    @Override
    public byte[] serialize(String s, User user) {
        if(user == null){
            return null;
        }
        byte[] idBytes,nameBytes;

        try {
            String id = user.getId();
            String name = user.getName();
            if(id != null){
               idBytes = id.getBytes("UTF-8");

            }else{
                idBytes = new byte[0];
            }

            if(name != null){
                nameBytes = name.getBytes("UTF-8");

            }else{
                nameBytes = new byte[0];
            }

            ByteBuffer byteBuffer = ByteBuffer.allocate(4+4+idBytes.length+nameBytes.length);
            byteBuffer.putInt(idBytes.length);
            byteBuffer.put(idBytes);

            byteBuffer.putInt(nameBytes.length);
            byteBuffer.put(nameBytes);
            return byteBuffer.array();
        }catch (UnsupportedEncodingException e){

        }

        return new byte[0];
    }

    @Override
    public void close() {

    }
}
