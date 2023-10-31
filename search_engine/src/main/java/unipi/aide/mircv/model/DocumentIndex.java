package unipi.aide.mircv.model;

import unipi.aide.mircv.exceptions.DocumentNotFoundException;
import unipi.aide.mircv.exceptions.UnableToAddDocumentIndexException;

import java.io.*;

public class DocumentIndex {

    private static final String FILE_PATH = "invertedIndex/documentIndex";

    public static void add(long docid, String docno, int docLen) throws UnableToAddDocumentIndexException {
        try (DataOutputStream dataOutputStream = new DataOutputStream(new FileOutputStream(FILE_PATH, true))) {
            dataOutputStream.writeUTF(docno);   // Write  docno as UTF-8 formatted string
            dataOutputStream.writeLong(docid);
            dataOutputStream.writeInt(docLen);

        } catch (IOException e) {
            throw new UnableToAddDocumentIndexException("Unable to write to document index file.");
        }
    }

    public static DocumentInfo retrieve(String docno) throws DocumentNotFoundException {
        try (DataInputStream dataInputStream = new DataInputStream(new FileInputStream(FILE_PATH))) {
            while (dataInputStream.available() > 0) {
                String storedDocno = dataInputStream.readUTF();
                long docid = dataInputStream.readLong();
                int docLen = dataInputStream.readInt();
                if (docno.equals(storedDocno)) {
                    return new DocumentInfo(docid, docLen);
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
