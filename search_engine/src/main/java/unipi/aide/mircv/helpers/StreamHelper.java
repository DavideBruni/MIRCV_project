package unipi.aide.mircv.helpers;

import java.io.File;
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

    public static void createDir(String Path){
        File directory = new File(Path);
        if (!directory.exists()) {
            if (!directory.mkdirs()) {
                throw new RuntimeException("Not able to create the directory: " + Path);
            }
        }
    }
}
