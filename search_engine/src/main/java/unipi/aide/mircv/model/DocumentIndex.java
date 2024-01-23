package unipi.aide.mircv.model;

import unipi.aide.mircv.configuration.Configuration;
import unipi.aide.mircv.exceptions.DocumentNotFoundException;
import unipi.aide.mircv.exceptions.UnableToWriteDocumentIndexException;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;

public class DocumentIndex {

    private static final String FILE_NAME = "documentIndex.dat";
    // the index could be a simple Array since:
    //      - documentIds are incremental
    //      - I choose to have the value of the docno = docid (DocId are integers in the collection)
    //  but since I don't know the size a priori I need an ArrayList
    private static final ArrayList<Integer> index = new ArrayList<>();


    /**
     * Adds a document length to the index.
     *
     * @param docLen The length of the document to be added to the index.
     **/
    public static void add(int docLen) {
        index.add(docLen);      // I save only the document length
    }

    /**
     * Writes the document lengths stored in the index to disk.
     */
    public static void writeOnDisk() throws UnableToWriteDocumentIndexException {
        try(FileChannel stream = (FileChannel) Files.newByteChannel(Path.of(Configuration.getDocumentIndexPath() + FILE_NAME), StandardOpenOption.APPEND, StandardOpenOption.CREATE)){
            ByteBuffer buffer = ByteBuffer.allocateDirect(Integer.BYTES * index.size());
            for(int i : index){
                buffer.putInt(i);
            }
            buffer.flip();
            stream.write(buffer);
        } catch (IOException e) {
            throw new UnableToWriteDocumentIndexException(e.getMessage());
        }
    }

    /**
     * Reads document lengths from disk and populates the index.
     */
    public static void loadFromDisk(){
        int numEntries = CollectionStatistics.getCollectionSize();
        try(FileChannel stream = (FileChannel) Files.newByteChannel(Path.of(Configuration.getDocumentIndexPath() + FILE_NAME), StandardOpenOption.READ)){
            ByteBuffer buffer = ByteBuffer.allocateDirect(Integer.BYTES * numEntries);
            stream.read(buffer);
            buffer.flip();
            for(int i = 0; i< numEntries; i++){
                index.add(buffer.getInt());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Retrieves the document length for a given document ID.
     *
     * @param docIdToFind The document ID to find the length for.
     * @return The length of the document with the specified ID.
     * @throws DocumentNotFoundException If the document with the specified ID is not found.
     */
    public static int getDocumentLength(int docIdToFind) throws DocumentNotFoundException {
        try {
            return index.get(docIdToFind - 1);       //docId starts from 1, so i need to check for docId - 1
        }catch (Exception e){
            throw new DocumentNotFoundException(e.getMessage());
        }
    }

    public static String getDocno(int docid) {
        return String.valueOf(docid - 1);
    }
}
