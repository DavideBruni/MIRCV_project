package unipi.aide.mircv.model;

import unipi.aide.mircv.exceptions.DocumentNotFoundException;
import unipi.aide.mircv.exceptions.UnableToAddDocumentIndexException;
import unipi.aide.mircv.helpers.FileHelper;
import unipi.aide.mircv.log.CustomLogger;

import java.io.*;

public class DocumentIndex {

    private static final String DIR_PATH = "data/invertedIndex";
    private static final String FILE_NAME = "invertedIndex.dat";

    /***
     * Add mapping between docId and docno and document length to the Document Index, written on Disk
     *
     * @param docid         docId of the document to save
     * @param docno         pid (aka docno) of the document to save
     * @param docLen        length of the document to save
     * @throws UnableToAddDocumentIndexException
     */
    public static void add(long docid, String docno, int docLen) throws UnableToAddDocumentIndexException {
        FileHelper.createDir(DIR_PATH);
        try (DataOutputStream dataOutputStream = new DataOutputStream(new FileOutputStream(DIR_PATH+FILE_NAME, true))) {
            // Write docno as integer, then treated as a string, because in our collection pid are integers, save space by storing as Integers
            dataOutputStream.writeInt(Integer.parseInt(docno));
            dataOutputStream.writeLong(docid);
            dataOutputStream.writeInt(docLen);

        } catch (IOException e) {
            CustomLogger.error("Unable to write to document index file.");
            throw new UnableToAddDocumentIndexException("Unable to write to document index file.");
        }
    }

    // TODO for now using only for testing purpose
    public static DocumentInfo retrieve(String docno) throws DocumentNotFoundException {
        try (DataInputStream dataInputStream = new DataInputStream(new FileInputStream(DIR_PATH+FILE_NAME))) {
            while (dataInputStream.available() > 0) {
                String storedDocno = Integer.toString(dataInputStream.readInt());
                long docid = dataInputStream.readLong();
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
     * @throws DocumentNotFoundException
     */
    public static int getDocumentLength(long docIdToFind) throws DocumentNotFoundException {
        try (DataInputStream dataInputStream = new DataInputStream(new FileInputStream(DIR_PATH+FILE_NAME))) {
            while (dataInputStream.available() > 0) {
                dataInputStream.skipBytes(Integer.BYTES);
                long docid = dataInputStream.readLong();
                int docLen = dataInputStream.readInt();
                if (docIdToFind == docid) {
                    return docLen;
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
}
