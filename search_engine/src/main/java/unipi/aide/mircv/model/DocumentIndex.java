package unipi.aide.mircv.model;

import unipi.aide.mircv.exceptions.DocumentNotFoundException;
import unipi.aide.mircv.exceptions.UnableToAddDocumentIndexException;
import unipi.aide.mircv.fileHelper.FileHelper;

import java.io.*;

public class DocumentIndex {

    private static final String FILE_PATH = "data/invertedIndex";

    public static void add(long docid, String docno, int docLen) throws UnableToAddDocumentIndexException {
        try (DataOutputStream dataOutputStream = new DataOutputStream(new FileOutputStream(FILE_PATH, true))) {
            dataOutputStream.writeInt(Integer.parseInt(docno));   // Write docno as integer, then treated as a string, because in our collection pid are integers
            dataOutputStream.writeLong(docid);
            dataOutputStream.writeInt(docLen);

        } catch (IOException e) {
            throw new UnableToAddDocumentIndexException("Unable to write to document index file.");
        }
    }

    public static DocumentInfo retrieve(String docno) throws DocumentNotFoundException {
        FileHelper.createDir(FILE_PATH);
        try (DataInputStream dataInputStream = new DataInputStream(new FileInputStream(FILE_PATH+"/invertedIndex"))) {
            while (dataInputStream.available() > 0) {
                String storedDocno = String.valueOf(dataInputStream.readInt());
                long docid = dataInputStream.readLong();
                int docLen = dataInputStream.readInt();
                if (docno.equals(storedDocno)) {
                    return new DocumentInfo(storedDocno,docid, docLen);
                }
            }
        } catch (FileNotFoundException e) {
            throw new DocumentNotFoundException("Document index file not found.");
        } catch (IOException e) {
            throw new DocumentNotFoundException("Document not found in the index.");
        }
        throw new DocumentNotFoundException("Document not found in the index.");
    }
}
