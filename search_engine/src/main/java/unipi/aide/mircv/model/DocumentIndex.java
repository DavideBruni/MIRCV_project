package unipi.aide.mircv.model;

import unipi.aide.mircv.configuration.Configuration;
import unipi.aide.mircv.exceptions.DocumentNotFoundException;
import unipi.aide.mircv.exceptions.UnableToAddDocumentIndexException;
import unipi.aide.mircv.helpers.StreamHelper;
import unipi.aide.mircv.log.CustomLogger;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;


public class DocumentIndex {

    private static final String FILE_NAME = "documentIndex.dat";
    private static FileChannel stream;
    private static final int ENTRY_SIZE = 8;
    private static int numEntries;
    private static Map<Integer,Integer> index = new HashMap<>();

    private static void initializeOutputStream() throws IOException {
        stream = (FileChannel) Files.newByteChannel(Path.of(Configuration.getDocumentIndexPath() + FILE_NAME), StandardOpenOption.APPEND, StandardOpenOption.CREATE);
    }


    public static void add(int docno, int docLen) throws UnableToAddDocumentIndexException {
        StreamHelper.createDir(Configuration.getDocumentIndexPath());
        try {
            ByteBuffer buffer = ByteBuffer.allocateDirect(ENTRY_SIZE);
            if(stream == null)
                initializeOutputStream();
            // Write docno as integer, then treated as a string, because in our collection pid are integers, save space by storing as Integers
            buffer.putInt(docno);
            buffer.putInt(docLen);
            buffer.flip();
            stream.write(buffer);
        } catch (IOException e) {
            CustomLogger.error("Unable to write to document index file.");
            throw new UnableToAddDocumentIndexException("Unable to write to document index file.");
        }
    }

    public static void closeStream(){
        try{
            stream.close();
        }catch (IOException e) {
            CustomLogger.error("Error while closing DocumentIndexOutputStream");
        }catch (NullPointerException ne){}
    }


    /***
     *
     * @param docIdToFind   the docId of the document to which you want to know the length
     * @return              the length of the document as number of tokens in it
     */
    public static int getDocumentLength(int docIdToFind) throws DocumentNotFoundException {
        Integer documentLength = index.get(docIdToFind);
        if(numEntries == 0)
            numEntries = CollectionStatistics.getCollectionSize();
        if (documentLength == null) {
            long low = 0;
            long high = numEntries;
            long mid;

            try (FileChannel file = (FileChannel) Files.newByteChannel(Path.of(Configuration.getDocumentIndexPath()+FILE_NAME), StandardOpenOption.READ)) {
                while (low <= high) {
                    mid = (low + high) >>> 1;

                    file.position(mid * ENTRY_SIZE);       // position at mid-file (or mid "partition" if it's not the first iteration)

                    // read the record and parse it to string
                    ByteBuffer buffer = ByteBuffer.allocateDirect(ENTRY_SIZE);
                    file.read(buffer);
                    buffer.flip();
                    int docId = buffer.getInt();
                    documentLength = buffer.getInt();
                    if (!index.containsKey(docId))
                        index.put(docId, documentLength);

                    if (docId == docIdToFind) {       //find the entry
                        return documentLength;
                    } else if (docId < docIdToFind) {     // current entry is lower than target one
                        low = mid + 1;
                    } else {                // current entry is greater than target one
                        high = mid - 1;
                    }
                }

            } catch (IOException e) {
                throw new DocumentNotFoundException("Document not found in the index.");
            }
            CustomLogger.error("Document not found in the index.");
            throw new DocumentNotFoundException("Document not found in the index.");
        } else {
            return documentLength;
        }
    }

    public static String getDocno(int docid) {
        return String.valueOf(docid);
    }
}
