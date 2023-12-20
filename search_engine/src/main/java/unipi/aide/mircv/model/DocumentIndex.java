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
    private static final int ENTRY_SIZE = 12;
    private static int numEntries;
    private static Map<Integer,DocumentInfo> index = new HashMap<>(9200000,0.97f);

    private static void initializeOutputStream() throws IOException {
        stream = (FileChannel) Files.newByteChannel(Path.of(Configuration.getDocumentIndexPath() + FILE_NAME), StandardOpenOption.APPEND, StandardOpenOption.CREATE);
    }


    /***
     * Add mapping between docId and docno and document length to the Document Index, written on Disk
     *
     * @param docid         docId of the document to save
     * @param docno         pid (aka docno) of the document to save
     * @param docLen        length of the document to save
     */
    public static void add(int docid, String docno, int docLen) throws UnableToAddDocumentIndexException {
        StreamHelper.createDir(Configuration.getDocumentIndexPath());
        try {
            ByteBuffer buffer = ByteBuffer.allocateDirect(12);
            if(stream == null)
                initializeOutputStream();
            // Write docno as integer, then treated as a string, because in our collection pid are integers, save space by storing as Integers
            buffer.putInt(Integer.parseInt(docno));
            buffer.putInt(docid);
            buffer.putInt(docLen);
            buffer.flip();
            stream.write(buffer);
        } catch (IOException e) {
            CustomLogger.error("Unable to write to document index file.");
            throw new UnableToAddDocumentIndexException("Unable to write to document index file.");
        }
    }

    public static void closeStreams(){
        try{
            stream.close();
        }catch (IOException e) {
            CustomLogger.error("Error while closing DocumentIndexOutputStream");
        }catch (NullPointerException ne){}
    }


    // TODO for now using only for testing purpose
    public static DocumentInfo retrieve(String docno) throws DocumentNotFoundException {
        try (DataInputStream dataInputStream = new DataInputStream(new FileInputStream(Configuration.getDocumentIndexPath()+FILE_NAME))) {
            while (dataInputStream.available() > 0) {
                String storedDocno = Integer.toString(dataInputStream.readInt());
                int docid = dataInputStream.readInt();
                int docLen = dataInputStream.readInt();
                if (docno.equals(storedDocno)) {
                    return new DocumentInfo(storedDocno,docid, docLen);
                }
            }
        } catch (FileNotFoundException e) {
            CustomLogger.error("Document index file not found.");
            throw new DocumentNotFoundException("Document index file not found.");
        } catch (IOException e) {
            CustomLogger.error("Document not found in the index.");
            throw new DocumentNotFoundException("Document not found in the index.");
        }
        CustomLogger.error("Document not found in the index.");
        throw new DocumentNotFoundException("Document not found in the index.");
    }

    /***
     *
     * @param docIdToFind   the docId of the document to which you want to know the length
     * @return              the length of the document as number of tokens in it
     */
    public static int getDocumentLength(int docIdToFind) throws DocumentNotFoundException {
        DocumentInfo documentInfo = index.get(docIdToFind);
        if(numEntries == 0)
            numEntries = CollectionStatistics.getCollectionSize();
        if (documentInfo == null) {
            long low = 0;
            long high = numEntries;
            long mid;

            try (FileChannel file = (FileChannel) Files.newByteChannel(Path.of(Configuration.getDocumentIndexPath()+FILE_NAME), StandardOpenOption.READ)) {
                while (low <= high) {
                    mid = (low + high) >>> 1;

                    file.position(mid * ENTRY_SIZE);       // position at mid-file (or mid "partition" if it's not the first iteration)

                    // read the record and parse it to string
                    ByteBuffer buffer = ByteBuffer.allocateDirect(12);
                    file.read(buffer);
                    buffer.flip();
                    String docno = String.valueOf(buffer.getInt());
                    int docId = buffer.getInt();
                    int documentLength = buffer.getInt();
                    documentInfo = new DocumentInfo(docno, docId, documentLength);
                    if (!index.containsKey(docId))
                        index.put(docId, documentInfo);

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
            return documentInfo.getDocLen();
        }
    }

    public static String getDocno(int docid) {
        return index.get(docid).getPid();
    }
}
