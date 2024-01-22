package unipi.aide.mircv.model;

import java.util.ArrayList;
import java.util.List;

public class UnaryCompressor {

    /**
     * Calculates the size, in bytes, needed to represent a list of frequencies in unary encoding.
     * Unary encoding represents each element of the list by a sequence of '1's followed by a '0'
     *
     * @param frequencies A list of integers representing the frequencies of elements.
     * @return The size, in bytes, required for the unary encoding of the given list of frequencies.
     *         If the calculated size is 0, it is adjusted to 1 to represent at least one byte.
     */
    private static int getByteSizeInUnary(List<Integer> frequencies) {
        // Calculate the sum of list elements
        int sum = frequencies.stream().mapToInt(Integer::intValue).sum();
        // Consider n 0s (one for each element)
        sum += frequencies.size();
        int size = (int) Math.ceil((double)sum/Byte.SIZE); // Calculate the size in bytes, rounding up to the nearest integer
        // Ensure the size is at least 1 byte
        return size == 0 ? 1 : size;
    }

    /**
     * Compresses a list of frequencies using unary encoding.
     *
     * @param frequencies A list of integers representing the frequencies of elements.
     * @return A byte array containing the compressed unary encoding of the given list of frequencies.
     *         The size of the byte array is determined by the {@link #getByteSizeInUnary(List)} method.
     * @see #getByteSizeInUnary(List)
     */
    public static byte[] compress(List<Integer> frequencies) {
        byte[] compressedFrequencies = new byte [getByteSizeInUnary(frequencies)];
        long bitsOffset = 0;
        for (Integer frequency : frequencies) {
            bitsOffset = Bits.writeUnary(compressedFrequencies, bitsOffset, frequency) + 1;
        }
        return compressedFrequencies;
    }

    /**
     * Decompresses frequencies from a byte array using unary encoding.
     * This method takes a byte array containing compressed frequencies in unary encoding
     * and decompresses them into a list of integers.
     *
     * @param compressedFrequencies A byte array containing the compressed unary encoding of frequencies.
     * @return A list of integers representing the decompressed frequencies.
     * @see Bits#readUnary(byte[], long)
     */
    public static List<Integer> decompressFrequencies(byte [] compressedFrequencies) {
        List<Integer> frequencies = new ArrayList<>();
        long bitsOffset = 0;
        for(int i = 0; i<compressedFrequencies.length; i++){
            int number = Bits.readUnary(compressedFrequencies,bitsOffset);
            frequencies.add(number);
            bitsOffset += number + 1;
        }
        return frequencies;
    }

    /**
     * This method reads the unary-encoded frequencies from the given compressed byte array,
     * starting from the specified index and continuing until the target index is reached.
     * It returns an array containing the last read frequency and the updated index.
     *
     * @param compressedIds         A byte array containing compressed unary-encoded frequencies.
     * @param index                 The target index representing the frequency to retrieve.
     * @param lastReadFrequency     The last frequency that was read (starting point for reading).
     * @param currentFrequencyIndex The current index in the compressed byte array for frequency decoding.
     * @return An array containing two elements:
     *         - The last read frequency.
     *         - The updated index (position) in the compressed byte array for further decoding.
     * @see Bits#readUnary(byte[], long)
     */
    public static long[] get(byte[] compressedIds, int index, int lastReadFrequency, long currentFrequencyIndex) {
        int number = 0;
        // I need double loop since can I had skipped some docIds so the last frequency I read is different from the previous
        // element in the array
        for(; lastReadFrequency<index; lastReadFrequency++) {       // from lastDecompressNumber, to the actual one
            number = 0;
            for (; currentFrequencyIndex<compressedIds.length; currentFrequencyIndex++) {
                number = Bits.readUnary(compressedIds,currentFrequencyIndex);
                currentFrequencyIndex += number + 1;
            }
        }
        return new long[] {number,currentFrequencyIndex};
    }


}
