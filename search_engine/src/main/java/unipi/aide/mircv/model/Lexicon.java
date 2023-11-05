package unipi.aide.mircv.model;

import unipi.aide.mircv.fileHelper.FileHelper;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class Lexicon {
    private static String TEMP_PATH ="data/temp/lexicon";
    private static int NUM_FILE_WRITTEN = 0;

    private Map<String,LexiconEntry> entries = null;

    public Lexicon(){
        entries = new HashMap<>();
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
            entries.put(token,tmp);
        }
    }

    public void updatefrequencySize(String token, int offset) {
        if (contains(token)) {
            LexiconEntry tmp = entries.get(token);
            tmp.setDocIdOffset(offset);
            entries.put(token,tmp);
        }
    }

    public void writeToDisk() {
        FileHelper.createDir(TEMP_PATH);
        File file = new File(TEMP_PATH + "/part" + NUM_FILE_WRITTEN);
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
                dataOutputStream.writeInt(tmp.getNumBlocks());
            }
            NUM_FILE_WRITTEN++;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static Lexicon readFromDisk(int partition) {
        int i = 0;
        String fileName = TEMP_PATH + "/part"+partition;
        try (DataInputStream dataInputStream = new DataInputStream(new FileInputStream(fileName))) {
            Map<String, LexiconEntry> lexicon = new HashMap<>();
            while (true) {
                try{
                    int key_length = dataInputStream.readInt();
                    String key = new String(dataInputStream.readNBytes(key_length), StandardCharsets.UTF_8);
                    LexiconEntry tmp = new LexiconEntry();
                    tmp.setDf(dataInputStream.readInt());
                    tmp.setIdf(dataInputStream.readInt());
                    tmp.setDocIdOffset(dataInputStream.readInt());
                    tmp.setFrequencyOffset(dataInputStream.readInt());
                    tmp.setNumBlocks(dataInputStream.readInt());
                    lexicon.put(key,tmp);

                }catch (EOFException eof){
                    break;
                }
            }
            lexicon = new LinkedHashMap<>(
                    lexicon.entrySet()
                            .stream()
                            .sorted(Map.Entry.comparingByKey())
                            .collect(
                                    Collectors.toMap(
                                            Map.Entry::getKey,
                                            Map.Entry::getValue,
                                            (e1, e2) -> e1, LinkedHashMap::new)
                            )
            );
            return new Lexicon(lexicon);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void clear() {
        entries = new HashMap<>();
    }
}
