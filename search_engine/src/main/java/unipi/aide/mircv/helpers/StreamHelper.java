package unipi.aide.mircv.helpers;

import unipi.aide.mircv.log.CustomLogger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;


public class StreamHelper {
    public static void writeInt(FileOutputStream fos, int integer) throws IOException {
        fos.write((integer >> 24) & 0xFF);
        fos.write((integer >> 16) & 0xFF);
        fos.write((integer >> 8) & 0xFF);
        fos.write(integer & 0xFF);
    }

    public static void createDir(String Path){
        File directory = new File(Path);
        if (!directory.exists()) {
            if (!directory.mkdirs()) {
                throw new RuntimeException("Not able to create the directory: " + Path);
            }
        }
    }

    public static void deleteDir(Path path) {

        // Elimina i file dentro temp
        deleteFilesInDirectory(path);

        // Elimina le directory docIds, frequencies e lexicon dentro temp
        deleteSubDirectories(path, "docIds");
        deleteSubDirectories(path, "frequencies");
        deleteSubDirectories(path, "lexicon");

        // Elimina la directory temp
        try {
            Files.delete(path);
            // CustomLogger.info("Directory deleted: " + path);
        } catch (IOException e) {
            // CustomLogger.error("Error deleting directory " + path + ": " + e.getMessage());
        }
    }

    private static void deleteFilesInDirectory(Path directory) {
        try {
            Files.walkFileTree(directory, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    // CustomLogger.info("File deleted: " + file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc){
                    CustomLogger.error("Error while visiting file " + file + ": " + exc.getMessage());
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            CustomLogger.error("Error deleting files in directory " + directory + ": " + e.getMessage());
        }
    }

    private static void deleteSubDirectories(Path parentDirectory, String subDirectoryName) {
        Path subDirectory = parentDirectory.resolve(subDirectoryName);
        deleteFilesInDirectory(subDirectory);
        try {
            Files.delete(subDirectory);
            //CustomLogger.info("Directory deleted: " + subDirectory);
        } catch (IOException e) {
            CustomLogger.error("Error deleting directory " + subDirectory + ": " + e.getMessage());
        }
    }
}
