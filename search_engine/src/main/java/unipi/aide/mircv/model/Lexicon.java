package unipi.aide.mircv.model;

import unipi.aide.mircv.configuration.Configuration;
import unipi.aide.mircv.helpers.FileHelper;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Function;

public class Lexicon {

    private static final String TEMP_DIR ="data/invertedIndex/temp/lexicon";
    
    private static int NUM_FILE_WRITTEN = 0;

    private Map<String,LexiconEntry> entries;

    private static Lexicon instance;

    public static Lexicon getInstance(){
        if(instance == null){
            instance = new Lexicon();
        }
        return instance;
    }

    private Lexicon(){
        entries = new TreeMap<>();
    }

    private Lexicon(Map<String, LexiconEntry> map) {
        entries = map;
    }


    public static String[] getFirstTokens(DataInputStream[] partialLexiconStreams) {
        return getTokens(new String[NUM_FILE_WRITTEN],partialLexiconStreams);
    }

    public static String[] getTokens(String[] tokens, DataInputStream[] partialLexiconStreams){
        for(int i =0; i< tokens.length; i++){
            if (tokens[i] == null && partialLexiconStreams[i] != null){
                try {
                    tokens[i] = partialLexiconStreams[i].readUTF();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

            }
        }
        return tokens;
    }


    public static DataInputStream[] getStreams() {
        DataInputStream[] dataInputStreams = new DataInputStream[NUM_FILE_WRITTEN];
        for(int i=0; i<NUM_FILE_WRITTEN; i++){
            try {
                dataInputStreams[i] = new DataInputStream(new FileInputStream(TEMP_DIR + "/part" + i + ".dat"));
            } catch (FileNotFoundException e) {
                // print: impossibile aprire input stream per partition i, partialLexicon lost
            }
        }
        return dataInputStreams;
    }

    public static LexiconEntry readEntry(DataInputStream partialLexiconStream) {
        LexiconEntry lexiconEntry = new LexiconEntry();
        try {
            lexiconEntry.setDf(partialLexiconStream.readInt());
            lexiconEntry.setIdf(partialLexiconStream.readDouble());
            lexiconEntry.setDocIdOffset(partialLexiconStream.readInt());
            lexiconEntry.setFrequencyOffset(partialLexiconStream.readInt());
            lexiconEntry.setNumBlocks(partialLexiconStream.readInt());
        } catch (EOFException eof){
            eof.printStackTrace();
            lexiconEntry = null;
        } catch (IOException e) {
            // print log error
            lexiconEntry = null;
        }
        return lexiconEntry;
    }

    public static <T> T getEntryValue(String term, Function<LexiconEntry, T> valueExtractor) {
        LexiconEntry lexiconEntry = instance.getEntry(term);
        if (lexiconEntry!= null)
            return valueExtractor.apply(lexiconEntry);
        return null;
    }

    public boolean contains(String token) {
        return entries.containsKey(token);
    }

    public LexiconEntry getEntry(String token) {
        LexiconEntry res = entries.get(token);
        if(res==null){
            res = getEntryFromDisk(token);
        }
        return res;
    }

    private LexiconEntry getEntryFromDisk(String targetToken) {
        int entrySize = (CollectionStatistics.getLongestTermLength() + 28);
        long low = 0;
        long high = CollectionStatistics.getNumberOfTokens();
        long mid = (low + high) >>> 1;

        try(RandomAccessFile file = new RandomAccessFile(Configuration.LEXICON_PATH, "r")){
            while (low <= high) {
                mid = (low + high) >>> 1;

                // Posizionati alla metà del record più vicino
                file.seek(mid*entrySize);

                // Leggi il record effettivo
                byte[] recordBytes = new byte[CollectionStatistics.getLongestTermLength()];
                file.read(recordBytes);

                // Converte il record in stringa
                String currentToken = new String(recordBytes, StandardCharsets.UTF_8).trim();

                int compareResult = currentToken.compareTo(targetToken);

                if (compareResult == 0) {
                    return new LexiconEntry(file.readInt(), file.readDouble(), file.readInt(),
                            file.readInt(), file.readInt(), file.readInt());
                } else if (compareResult < 0) {
                    low = mid + 1;
                } else {
                    high = mid - 1;
                }
            }


        }catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
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

    public static void writeToDisk(boolean is_merged) {
        String filename;
        if(is_merged) {
            filename = Configuration.LEXICON_PATH;
        }else {
            FileHelper.createDir(TEMP_DIR);
            filename = TEMP_DIR + "/part" + NUM_FILE_WRITTEN+ ".dat";
        }
        File file = new File(filename);
        try (DataOutputStream dataOutputStream = new DataOutputStream(new FileOutputStream(file))) {
            for (String key : instance.entries.keySet()) {
                if(is_merged)
                    dataOutputStream.write(stringToArrayByteFixedDim(key,CollectionStatistics.getLongestTermLength()));
                else
                    dataOutputStream.writeUTF(key);
                LexiconEntry tmp = instance.entries.get(key);
                dataOutputStream.writeInt(tmp.getDf());
                dataOutputStream.writeDouble(tmp.getIdf());
                dataOutputStream.writeInt(tmp.getDocIdOffset());
                dataOutputStream.writeInt(tmp.getFrequencyOffset());
                dataOutputStream.writeInt(tmp.getNumBlocks());
                dataOutputStream.writeInt(tmp.getSkipPointerOffset());
                CollectionStatistics.setLongestTerm(key.length());
            }
            if (!is_merged)
                NUM_FILE_WRITTEN++;
            else
                CollectionStatistics.setNumberOfToken(instance.entries.keySet().size());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static byte[] stringToArrayByteFixedDim(String key, int length) {
        byte[] byteArray = new byte[length];
        byte[] inputBytes = key.getBytes(StandardCharsets.UTF_8);

        // Copia i byte della stringa di input nel byte array risultante
        System.arraycopy(inputBytes, 0, byteArray, 0, Math.min(inputBytes.length, length));

        // Aggiungi padding con spazi se la stringa è più corta della lunghezza desiderata
        for (int i = inputBytes.length; i < length; i++) {
            byteArray[i] = (byte) ' ';
        }

        return byteArray;
    }

    static Lexicon readFromDisk(int partition) {
        int i = 0;
        String filename;
        if(partition == -1) {
            filename = Configuration.LEXICON_PATH;
        }else {
            filename = TEMP_DIR + "/part" + i + ".dat";
        }
        try (DataInputStream dataInputStream = new DataInputStream(new FileInputStream(filename))) {
            Map<String, LexiconEntry> lexicon = new TreeMap<>();
            while (true) {
                try{
                    String key = dataInputStream.readUTF();
                    LexiconEntry tmp = new LexiconEntry();
                    tmp.setDf(dataInputStream.readInt());
                    tmp.setIdf(dataInputStream.readDouble());
                    tmp.setDocIdOffset(dataInputStream.readInt());
                    tmp.setFrequencyOffset(dataInputStream.readInt());
                    tmp.setNumBlocks(dataInputStream.readInt());
                    tmp.setSkipPointerOffset(dataInputStream.readInt());
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

    public static void clear() {
        instance.entries = new TreeMap<>();
    }

    public void add(String token, LexiconEntry lexiconEntry) {
        entries.put(token, lexiconEntry);       //used in mergedLexicon
    }

    public String getEntryAtPointer(int pointer) {
        try{
            Set<String> keys = entries.keySet();
            List<String> keyList = new ArrayList<>(keys);
            return keyList.get(pointer);
        }catch (IndexOutOfBoundsException e) {
            return null;
        }
    }

    public void setEntry(String token, LexiconEntry lexiconEntry){
        entries.put(token,lexiconEntry);
    }

    public void setNumberOfPostings(String token, int size) {
        entries.get(token).setPostingNumber(size);
    }
}
