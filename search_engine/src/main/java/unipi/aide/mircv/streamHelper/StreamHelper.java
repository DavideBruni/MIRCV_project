package unipi.aide.mircv.streamHelper;

import java.io.FileOutputStream;
import java.io.IOException;


public class StreamHelper {
    public static void writeInt(FileOutputStream fos, int integer) throws IOException {
        if(integer < Math.pow(2,24) - 1){
            fos.write(0);
        }
        if (integer < Math.pow(2,16) - 1) {
            fos.write(0);
        }
        if (integer < Math.pow(2,8) - 1) {
            fos.write(0);
        }
        fos.write(integer);
    }
}
