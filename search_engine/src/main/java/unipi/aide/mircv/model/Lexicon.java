package unipi.aide.mircv.model;

import unipi.aide.mircv.configuration.Configuration;
import unipi.aide.mircv.exceptions.PartialLexiconNotFoundException;
import unipi.aide.mircv.exceptions.UnableToWriteLexiconException;
import unipi.aide.mircv.helpers.StreamHelper;
import unipi.aide.mircv.log.CustomLogger;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.function.Function;

public class Lexicon{

    private static final String TEMP_DIR ="/invertedIndex/temp/lexicon";
    private static int NUM_FILE_WRITTEN = 0;        // needed to know how many lexicon retrieve in merge operation
    private Map<String,LexiconEntry> entries;       // for each token, we need to save several information
    private static final int LEXICON_CACHE_CAPACITY = 1000;

    private LinkedHashMap<String,LexiconEntry> lexiconCache;

    // singleton pattern
    private static Lexicon instance;
    public static Lexicon getInstance(){
        if(instance == null){
            instance = new Lexicon();
        }
        return instance;
    }

    private Lexicon(){
        entries = new TreeMap<>();
        lexiconCache  = new LinkedHashMap<>(LEXICON_CACHE_CAPACITY, 0.8f, true){
            protected boolean removeEldestEntry(Map.Entry<String, LexiconEntry> eldest)
            {
                return size() > LEXICON_CACHE_CAPACITY;
            }
        };
    }




    /**
     * Retrieves the first set of tokens from the specified array of partial lexicon streams.
     *
     * @param partialLexiconStreams An array of {@code DataInputStream} objects representing partial lexicon streams.
     * @return An array of strings containing the first set of tokens from the partial lexicon streams.
     * @throws PartialLexiconNotFoundException If an IOException occurs while reading from the input streams.
     */
    public static String[] getFirstTokens(FileChannel[] partialLexiconStreams) throws PartialLexiconNotFoundException {
        return getTokens(new String[NUM_FILE_WRITTEN],partialLexiconStreams);
    }


    /**
     * Populates the provided array of tokens by reading from the corresponding partial lexicon streams.
     * If a token is already present in the array, it is skipped. If the token is null, and the
     * corresponding partial lexicon stream is not null, the method attempts to read a token from the stream.
     *
     * @param tokens                 An array of strings to store the lexicon tokens.
     * @param partialLexiconStreams An array of {@code DataInputStream} objects representing partial lexicon streams.
     * @return The array of strings containing lexicon tokens, with updated values from the partial lexicon streams.
     * @throws PartialLexiconNotFoundException If an IOException occurs while reading from any of the input streams.
     */
    public static String[] getTokens(String[] tokens, FileChannel[] partialLexiconStreams) throws PartialLexiconNotFoundException {
        for(int i =0; i< tokens.length; i++){
            if (tokens[i] == null){
                try {
                    ByteBuffer tmp = ByteBuffer.allocateDirect(45);
                    int len = partialLexiconStreams[i].read(tmp);
                    if (len < 0)
                        throw new EOFException();
                    tmp.flip();
                    byte[] byteBuffer = new byte[45];
                    tmp.get(byteBuffer);
                    tokens[i] = new String(byteBuffer, 0, len);
                    tokens[i] = tokens[i].trim();
                }catch(EOFException e){
                    tokens[i] = null;
                } catch (IOException e) {
                    throw new PartialLexiconNotFoundException(e.getMessage());
                }
            }

        }
        return tokens;
    }

    /**
     * Retrieves an array of {@code DataInputStream} objects representing streams for lexicon partitions.
     *
     * @return An array of {@code DataInputStream} objects representing lexicon partition streams.
     */
    public static FileChannel[] getStreams() {
        FileChannel[] dataInputStreams = new FileChannel[NUM_FILE_WRITTEN];
        for(int i=0; i<NUM_FILE_WRITTEN; i++){
            try {
                dataInputStreams[i] = (FileChannel) Files.newByteChannel(Path.of(Configuration.getRootDirectory()+TEMP_DIR + "/part" + i + ".dat"));
            } catch (FileNotFoundException e) {
                CustomLogger.info("Unable to open stream for partition "+i+": partition lost, moving on");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return dataInputStreams;
    }


    public static LexiconEntry readEntryFromPartition(FileChannel partialLexiconStream) {
        // we already read the token stored, we have to read the remaning part of the LexiconEntry
        LexiconEntry lexiconEntry = new LexiconEntry();
        try {
            ByteBuffer buffer = ByteBuffer.allocateDirect(LexiconEntry.getEntryDimension(false));
            partialLexiconStream.read(buffer);
            buffer.flip();
            lexiconEntry.setDf(buffer.getInt());
            lexiconEntry.setDocIdOffset(buffer.getInt());
            lexiconEntry.setFrequencyOffset(buffer.getInt());
            lexiconEntry.setMaxId(buffer.getInt());
        } catch (EOFException eof){
            lexiconEntry = null;
        } catch (IOException e) {
            CustomLogger.error("Error while reading LexiconEntry: " + e.getMessage());
            lexiconEntry = null;
        }
        return lexiconEntry;
    }


    public static <T> T getEntryValue(String term, Function<LexiconEntry, T> valueExtractor) {
        LexiconEntry lexiconEntry = getEntry(term,true);
        if (lexiconEntry!= null)
            return valueExtractor.apply(lexiconEntry);
        return null;
    }



    /**
     * Retrieves the LexiconEntry for the specified token.
     * This method first attempts to retrieve the entry from memory. If it is not found, it is then
     * retrieved from the lexicon stored on disk using the {@link #getEntryFromDisk(String,boolean)} method.
     *
     * @param token The token for which to retrieve the LexiconEntry.
     * @return The LexiconEntry for the specified token, or null if the entry is not found.
     * @see #getEntryFromDisk(String,boolean)
     */
    public static LexiconEntry getEntry(String token,boolean is_merged) {
        LexiconEntry res = instance.lexiconCache.get(token);
        if (res == null) {
            if (instance.entries != null)
                res = instance.entries.get(token);
            if (res == null) {
                res = getEntryFromDisk(token, is_merged);
                instance.add(token, res);

            }
        }
        return res;
    }

    /**
     * Retrieves a LexiconEntry from disk performing a binary search.
     *
     * @param targetToken The token for which to retrieve the LexiconEntry from disk.
     * @return The LexiconEntry for the specified token, or null if the entry is not found.
     */
    private static LexiconEntry getEntryFromDisk(String targetToken, boolean is_merged) {
        int entrySize = (45 + LexiconEntry.getEntryDimension(is_merged));
        long low = 0;
        long high = CollectionStatistics.getNumberOfTokens();
        long mid;

        try(FileChannel stream = (FileChannel) Files.newByteChannel(Path.of(Configuration.getLexiconPath()), StandardOpenOption.READ)){
            while (low <= high) {
                mid = (low + high) >>> 1;

                stream.position(mid*entrySize); // position at mid-file (or mid "partition" if it's not the first iteration)

                // read the record and parse it to string
                ByteBuffer recordBytes = ByteBuffer.allocateDirect(45);
                int len = stream.read(recordBytes);
                recordBytes.flip();
                byte [] string_buffer = new byte[45];
                recordBytes.get(string_buffer);
                String currentToken = new String(string_buffer, 0, len).trim();

                int compareResult = currentToken.compareTo(targetToken);

                if (compareResult == 0) {       //find the entry
                    ByteBuffer buffer = ByteBuffer.allocateDirect(entrySize-45);
                    stream.read(buffer);
                    buffer.flip();
                    return new LexiconEntry(buffer.getInt(), buffer.getDouble(), buffer.getInt(),
                            buffer.getInt(), buffer.getInt(), buffer.getInt(), buffer.getDouble(), buffer.getDouble());
                } else if (compareResult < 0) {     // current entry is lower than target one
                    low = mid + 1;
                } else {                // current entry is greater than target one
                    high = mid - 1;
                }
            }


        }catch (IOException e) {
            CustomLogger.error("Error while performing binary search on disk");
        }
        return null;
    }


    public void updateDf(String token) {
        LexiconEntry lexiconEntry= entries.get(token);
        if(lexiconEntry == null){
            lexiconEntry = new LexiconEntry();
            entries.put(token,lexiconEntry);
        }else {
            lexiconEntry.updateDF();
        }
    }

    public static void setEntry(String token, LexiconEntry lexiconEntry){
        instance.entries.put(token, lexiconEntry);
    }


    public int numberOfEntries() {
        return instance.entries.entrySet().size();
    }


    /**
     * Writes the lexicon entries to disk, either as a merged lexicon or as individual partition files.
     *
     * @param is_merged A boolean indicating whether the lexicon is to be written as a merged lexicon.
     */

    public static void writeOnDisk(boolean is_merged, boolean debug) throws UnableToWriteLexiconException {
        String filename;
        if(is_merged) {
            filename = Configuration.getLexiconPath();
        }else {
            StreamHelper.createDir(Configuration.getRootDirectory()+TEMP_DIR);
            filename = Configuration.getRootDirectory()+TEMP_DIR + "/part" + NUM_FILE_WRITTEN+ ".dat";
        }
        File file = new File(filename);
        try(FileChannel stream = (FileChannel) Files.newByteChannel(file.toPath(),
                StandardOpenOption.APPEND,
                StandardOpenOption.CREATE)){
            for (String key : instance.entries.keySet()) {
                LexiconEntry tmp = instance.entries.get(key);
                tmp.writeOnDisk(stream,key,is_merged);
            }
            if (!is_merged)
                NUM_FILE_WRITTEN++;
        } catch (IOException e) {
            throw new UnableToWriteLexiconException(e.getMessage());
        }
        if(debug){
            PrintStream originalOut = System.out;
            try {
                PrintStream fileStream = new PrintStream(new FileOutputStream("data/debug/lexicon.txt",true));
                System.setOut(fileStream);
                System.out.println(instance);

            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                System.setOut(originalOut);
            }
        }
    }

    static byte[] stringToArrayByteFixedDim(String key, int length) {
        byte[] byteArray = new byte[length];
        byte[] inputBytes = key.getBytes(StandardCharsets.UTF_8);

        // String's bytes copy into the byte array
        System.arraycopy(inputBytes, 0, byteArray, 0, Math.min(inputBytes.length, length));

        // Add padding if necessary
        for (int i = inputBytes.length; i < length; i++) {
            byteArray[i] = (byte) ' ';
        }

        return byteArray;
    }


    public static void clear() {
        instance.entries = new TreeMap<>();
    }

    public void add(String token, LexiconEntry lexiconEntry) {
        entries.put(token, lexiconEntry);       //used in mergedLexicon
    }


    @Override
    public String toString() {
        return "Lexicon{" +
                "entries=" + entries +
                '}';
    }

    public static LinkedHashMap<String, LexiconEntry> getLexiconCache() {
        return instance.lexiconCache;
    }

    public static void setLexiconCache(LinkedHashMap<String, LexiconEntry> lexiconCache) {
        instance.lexiconCache.putAll(lexiconCache);
    }
}
