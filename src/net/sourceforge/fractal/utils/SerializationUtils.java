package net.sourceforge.fractal.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class SerializationUtils {
    static public byte[] serializableToByteArray(Serializable s) throws IOException{
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out;
        out = new ObjectOutputStream(bos);
        out.flush();
        out.writeObject(s);
        out.flush();
        out.close();
        
        // Get the bytes of the serialized object
        return bos.toByteArray();
    }

    public static Serializable byteArrayToSerializable(byte[] value) throws IOException, ClassNotFoundException {
        ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(value));
        return (Serializable) in.readObject();
    }
}
