package net.sourceforge.fractal.utils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;



/**
 * @author lasaro
 */
public class Log extends DataOutputStream{

    /*
     * The next two methods are used to make disk writes inocuous. Leave them commented
     * to use stable storage.
     */
    
    /*
    public synchronized void write(byte[] arg0, int arg1, int arg2)
            throws IOException {
    }
    
    public synchronized void write(int arg0) throws IOException {
    }
    */
    
    
    
    
    private FileChannel channel;
    public Log(String logFileName) throws IOException{
        super(new FileOutputStream(logFileName));
        channel = ((FileOutputStream)out).getChannel();
    }
    
    public void force() throws IOException {
        //FIXME
        channel.force(false);
    }
    
    public static void stdOutLog(String s){
        System.out.println(s);
    }
    
    public DataInputStream getDataInputStream() throws IOException{
            return new DataInputStream(new FileInputStream(((FileOutputStream)out).getFD()));
    }
    
    public static void recursiveDelete(File f) {
        if(f.isDirectory())
            for(File f2 :f.listFiles())
                recursiveDelete(f2);
        f.delete();
    }
}
