package unipi.aide.mircv.helpers;

import java.io.File;

public class FileHelper {
    public static void createDir(String Path){
        File directory = new File(Path);
        if (!directory.exists()) {
            if (!directory.mkdirs()) {
                throw new RuntimeException("Not able to create the directory: " + Path);
            }
        }
    }
}
