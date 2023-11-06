package unipi.aide.mircv.model;

import unipi.aide.mircv.fileHelper.FileHelper;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class Lexicon {
    private static final String FINAL_PATH = "data/lexicon.dat";
    private static String TEMP_PATH ="data/temp/lexicon";
    private static int NUM_FILE_WRITTEN = 0;

    private Map<String,LexiconEntry> entries = null;

    public Lexicon(){
        entries = new TreeMap<>();
    }

    private Lexicon(Map<String, LexiconEntry> map) {
        entries = map;
    }


    public static List<Lexicon> getPartialLexicons() {
        List<Lexicon> res = new ArrayList<>();
        for(int i = 0; i<NUM_FILE_WRITTEN;i++){
            res.add(readFromDisk(i));
        }
        return res;
    }

    public boolean contains(String token) {
        return entries.containsKey(token);
    }

    public LexiconEntry getEntry(String token) {
        return entries.get(token);
    }

    public void add(String token) {
        entries.put(token,new LexiconEntry());
    }

    public void updateDf(String token) {
        entries.put(token, entries.get(token).updateDF());
    }

    public void updateDocIdOffset(String token, int offset) {
        if (contains(token)) {
            LexiconEntry tmp = entries.get(token);
            tmp.setDocIdOffset(offset);
            entries.put(token,tmp);
        }
    }

    public void updateFrequencyOffset(String token, int offset) {
        if (contains(token)) {
            LexiconEntry tmp = entries.get(token);
            tmp.setFrequencyOffset(offset);
            entries.put(token,tmp);
        }
    }

    public void updatedocIdSize(String token, int offset) {
        if (contains(token)) {
            LexiconEntry tmp = entries.get(token);
            tmp.setDocIdSize(offset);

        }
    }

    public void updatefrequencySize(String token, int offset) {
        if (contains(token)) {
            LexiconEntry tmp = entries.get(token);
            tmp.setFrequencySize(offset);

        }
    }

    public void writeToDisk(boolean is_merged) {
        String filename;
        if(is_merged) {
            filename = FINAL_PATH;
        }else {
            FileHelper.createDir(TEMP_PATH);
            filename = TEMP_PATH + "/part" + NUM_FILE_WRITTEN;
        }
        File file = new File(filename);
        try (DataOutputStream dataOutputStream = new DataOutputStream(new FileOutputStream(file))) {
            for (String key : entries.keySet()) {
                int byteLength = key.getBytes().length;
                dataOutputStream.writeInt(byteLength);
                dataOutputStream.write(key.getBytes());
                LexiconEntry tmp = entries.get(key);
                dataOutputStream.writeInt(tmp.getDf());
                dataOutputStream.writeDouble(tmp.getIdf());
                dataOutputStream.writeInt(tmp.getDocIdOffset());
                dataOutputStream.writeInt(tmp.getFrequencyOffset());
                dataOutputStream.writeInt(tmp.getDocIdSize());
                dataOutputStream.writeInt(tmp.getFrequencySize());
                dataOutputStream.writeInt(tmp.getNumBlocks());
            }
            if (!is_merged)
                NUM_FILE_WRITTEN++;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static Lexicon readFromDisk(int partition) {
        int i = 0;
        String filename;
        if(partition == -1) {
            filename = FINAL_PATH;
        }else {
            filename = TEMP_PATH + "/part" + i;
        }
        try (DataInputStream dataInputStream = new DataInputStream(new FileInputStream(filename))) {
            Map<String, LexiconEntry> lexicon = new TreeMap<>();
            while (true) {
                try{
                    int key_length = dataInputStream.readInt();
                    String key = new String(dataInputStream.readNBytes(key_length), StandardCharsets.UTF_8);
                    LexiconEntry tmp = new LexiconEntry();
                    tmp.setDf(dataInputStream.readInt());
                    tmp.setIdf(dataInputStream.readDouble());
                    tmp.setDocIdOffset(dataInputStream.readInt());
                    tmp.setFrequencyOffset(dataInputStream.readInt());
                    tmp.setDocIdSize(dataInputStream.readInt());
                    tmp.setFrequencySize(dataInputStream.readInt());
                    tmp.setNumBlocks(dataInputStream.readInt());
                    lexicon.put(key,tmp);

                }catch (EOFException eof){
                    break;
                }
            }
            return new Lexicon(lexicon);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void clear() {
        entries = new TreeMap<>();
    }

    public void add(String token, LexiconEntry lexiconEntry) {
        entries.put(token, lexiconEntry);       //used in mergedLexicon
    }

    public String getEntryAtPointer(int pointer) {
        try{
            Set<String> keys = entries.keySet();
            List<String> keyList = new ArrayList<>(keys);
            String key = keyList.get(pointer);
            return key;
        }catch (IndexOutOfBoundsException e) {
            return null;
        }
    }
}
