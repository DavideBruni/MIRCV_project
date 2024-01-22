package unipi.aide.mircv.model;

import unipi.aide.mircv.configuration.Configuration;
import unipi.aide.mircv.exceptions.PartialLexiconNotFoundException;
import unipi.aide.mircv.exceptions.UnableToWriteLexiconException;
import unipi.aide.mircv.helpers.StreamHelper;
import unipi.aide.mircv.log.CustomLogger;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class Lexicon{

    private static final String TEMP_DIR ="/invertedIndex/temp/lexicon";
    private static int NUM_FILE_WRITTEN = 0;        // needed to know how many lexicon retrieve in merge operation
    private Map<String,LexiconEntry> entries;       // for each token, we need to save several information
    public static final int TERM_DIMENSION = 64;    // number of byte

    /* ---------------------- SINGLETON PATTERN  -------------------- */
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

    /* ---------------------- END SINGLETON PATTERN  -------------------- */

    /* ---------------------- FUNCTIONS USED ONLY IN MERGE PART  -------------------- */
    /**
     * Retrieves the first tokens from an array of partial lexicon streams.
     * This method reads the first tokens from the specified array of partial lexicon streams
     * and returns them as an array of strings.
     *
     * @param partialLexiconStreams An array of file channels representing partial lexicon streams.
     * @return                      An array of strings containing the first tokens from each partial lexicon stream.
     * @throws PartialLexiconNotFoundException If an error occurs while reading the partial lexicon streams.
     */
    public static String[] getFirstTokens(FileChannel[] partialLexiconStreams) throws PartialLexiconNotFoundException {
        return getTokens(new String[NUM_FILE_WRITTEN],partialLexiconStreams);
    }

    /**
     * Retrieves the lowest token for each of partial lexicon streams.
     * This method reads tokens from the specified array of partial lexicon streams and updates
     * the provided array of tokens. If a token is already present in the array, it is skipped,
     * and the next token is read from the corresponding partial lexicon stream.
     *
     * @param tokens                An array of strings to store the tokens.
     * @param partialLexiconStreams An array of file channels representing partial lexicon streams.
     * @return                      The updated array of strings containing tokens from partial lexicon streams.
     * @throws PartialLexiconNotFoundException If an error occurs while reading the partial lexicon streams.
     */
    public static String[] getTokens(String[] tokens, FileChannel[] partialLexiconStreams) throws PartialLexiconNotFoundException {
        // I know the position is right, because I read first the token, then the rest of the entry, I cannot skip the read of the entry
        for(int i =0; i< tokens.length; i++){
            if (tokens[i] == null){
                try {
                    ByteBuffer tmp = ByteBuffer.allocateDirect(TERM_DIMENSION);
                    int len = partialLexiconStreams[i].read(tmp);
                    if (len < 0)
                        throw new EOFException();
                    tmp.flip();
                    byte[] byteBuffer = new byte[TERM_DIMENSION];
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
        // I already read the token stored, so I have to read the remaining part of the LexiconEntry
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

    /* ---------------------- END FUNCTIONS USED ONLY IN MERGE PART  -------------------- */

    /**
     * Retrieves a LexiconEntry for the given token.
     * This method first attempts to retrieve a LexiconEntry from the in-memory entries map.
     * If the entry is not found in memory, it is then retrieved from disk and added to the
     * in-memory cache for future access.
     *
     * @param token             The token for which to retrieve the LexiconEntry.
     * @param is_merged         A boolean indicating whether the lexicon is merged.
     * @return The LexiconEntry associated with the given token.
     *         If the entry is not found, null is returned.
     */
    public static LexiconEntry getEntry(String token,boolean is_merged) {
        LexiconEntry res = null;
        if (instance.entries != null)
            res = instance.entries.get(token);
        if (res == null) {
            res = getEntryFromDisk(token, is_merged);
            instance.add(token, res);
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
        int entrySize = (TERM_DIMENSION + LexiconEntry.getEntryDimension(is_merged));
        long low = 0;
        long high = CollectionStatistics.getNumberOfTokens();
        long mid;

        try(FileChannel stream = (FileChannel) Files.newByteChannel(Path.of(Configuration.getLexiconPath()), StandardOpenOption.READ)){
            /* ---------------------- BINARY SEARCH  -------------------- */
            while (low <= high) {
                mid = (low + high) >>> 1;

                stream.position(mid*entrySize); // position at mid-file (or mid "partition" if it's not the first iteration)

                // read the record and parse it to string
                ByteBuffer recordBytes = ByteBuffer.allocateDirect(TERM_DIMENSION);
                int len = stream.read(recordBytes);
                recordBytes.flip();
                byte [] string_buffer = new byte[TERM_DIMENSION];
                recordBytes.get(string_buffer);
                String currentToken = new String(string_buffer, 0, len).trim();

                int compareResult = currentToken.compareTo(targetToken);

                if (compareResult == 0) {       //find the entry
                    ByteBuffer buffer = ByteBuffer.allocateDirect(entrySize-TERM_DIMENSION);
                    stream.read(buffer);
                    buffer.flip();
                    return new LexiconEntry(buffer.getInt(), buffer.getInt(), buffer.getInt(),
                            buffer.getInt(), buffer.getInt(), buffer.getDouble(), buffer.getDouble());
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


    /**
     * Writes the lexicon entries to disk, either as a merged lexicon or as individual partition files.
     *
     * @param is_merged A boolean indicating whether the lexicon is to be written as a merged lexicon.
     */

    public static void writeOnDisk(boolean is_merged, boolean debug) throws UnableToWriteLexiconException {
        /* ------------- SETTING FILE NAME -------------*/
        String filename;
        if(is_merged) {
            filename = Configuration.getLexiconPath();
        }else {
            StreamHelper.createDir(Configuration.getRootDirectory()+TEMP_DIR);
            filename = Configuration.getRootDirectory()+TEMP_DIR + "/part" + NUM_FILE_WRITTEN+ ".dat";
        }
        /* ---------------------------------------------*/
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
        /* ---------------- DEBUG  ----------------*/
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
        /* -------------------------------------*/
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

}
