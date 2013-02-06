package net.sourceforge.fractal.replication.database;

import static net.sourceforge.fractal.replication.database.PSTORE.RECLEN;

/**
 * 
 * @author Nicolas Schiper
 * 
 *  Simulate the following C struct:
 *  struct Defrec {
 *   	u_int32_t   id;
 *   	u_int32_t   balance;
 *   	u_int8_t    pad[RECLEN - sizeof(int) - sizeof(int)];
 *   }
 */

public class Defrec {
	
	public byte[] data;
	
    public Defrec() {
        data = new byte[RECLEN];
    }

    // The byte order is our choice.
    //
    public static int get_int_in_array(byte[] array, int offset) {
        return
            ((0xff & array[offset + 0]) << 0)  |
            ((0xff & array[offset + 1]) << 8)  |
            ((0xff & array[offset + 2]) << 16) |
            ((0xff & array[offset + 3]) << 24);
    }

    // Note: Value needs to be long to avoid sign extension
   public static void set_int_in_array(byte[] array, int offset, int value) {
        array[offset + 0] = (byte)((value >> 0) & 0xff);
        array[offset + 1] = (byte)((value >> 8) & 0xff);
        array[offset + 2] = (byte)((value >> 16) & 0xff);
        array[offset + 3] = (byte)((value >> 24) & 0xff);
    }


    // The byte order is our choice.
    //
    public static long get_long_in_array(byte[] array, int offset) {
        return
            ((0xff & array[offset + 0]) << 0)  |
            ((0xff & array[offset + 1]) << 8)  |
            ((0xff & array[offset + 2]) << 16) |
            ((0xff & array[offset + 3]) << 24) |
			((0xff & array[offset + 4]) << 32) |
            ((0xff & array[offset + 5]) << 40) |
            ((0xff & array[offset + 6]) << 48) |
            ((0xff & array[offset + 7]) << 56);
    }

    // Note: Value needs to be long to avoid sign extension
    public static void set_long_in_array(byte[] array, int offset, long value) {
        array[offset + 0] = (byte)((value >> 0) & 0xff);
        array[offset + 1] = (byte)((value >> 8) & 0xff);
        array[offset + 2] = (byte)((value >> 16) & 0xff);
        array[offset + 3] = (byte)((value >> 24) & 0xff);
        array[offset + 4] = (byte)((value >> 32) & 0xff);
        array[offset + 5] = (byte)((value >> 40) & 0xff);
        array[offset + 6] = (byte)((value >> 48) & 0xff);
        array[offset + 7] = (byte)((value >> 56) & 0xff);		
    }
	
	
    public int length() {
        return RECLEN;
    }

    public long get_id() {
        return get_int_in_array(data, 0);
    }

    public void set_id(int value) {
        set_int_in_array(data, 0, value);
    }

    public int get_balance() {
        return get_int_in_array(data, 4);
    }

    public void set_balance(int value) {
        set_int_in_array(data, 4, value);
    }
	
	public void set_ts(int ts) {
		set_int_in_array(data, 8, ts);
	}
	
	public int get_ts() {
		return get_int_in_array(data, 8);
	}

    static {
        Defrec d = new Defrec();
        d.set_balance(500000);
    }
}
