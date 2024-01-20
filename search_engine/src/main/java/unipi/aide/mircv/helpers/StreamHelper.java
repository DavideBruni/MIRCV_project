package unipi.aide.mircv.helpers;

import unipi.aide.mircv.log.CustomLogger;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;


public class StreamHelper {
    public static void createDir(String Path){
        File directory = new File(Path);
        if (!directory.exists()) {
            if (!directory.mkdirs()) {
                throw new RuntimeException("Not able to create the directory: " + Path);
            }
        }
    }

    public static void deleteDir(Path path) {
        deleteFilesInDirectory(path);           // Delete files inside path
        // Delete the following directories inside path: docIds, frequencies, and lexicon
        deleteSubDirectories(path, "docIds");
        deleteSubDirectories(path, "frequencies");
        deleteSubDirectories(path, "lexicon");

        // Delete directory path
        try {
            Files.delete(path);
        } catch (IOException e) {
             CustomLogger.error("Error deleting directory " + path + ": " + e.getMessage());
        }
    }

    private static void deleteFilesInDirectory(Path directory) {
        try {
            Files.walkFileTree(directory, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
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
        } catch (IOException e) {
            CustomLogger.error("Error deleting directory " + subDirectory + ": " + e.getMessage());
        }
    }
}
