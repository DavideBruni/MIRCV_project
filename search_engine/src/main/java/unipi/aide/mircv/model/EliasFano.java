package unipi.aide.mircv.model;

import java.util.ArrayList;
import java.util.List;

public class EliasFano {


	/**
	 * This function roundUp the {@code val} value to the next integer multiple of Byte.SIZE
	 * Because I want that LowBits and HighBits end are multiple of a byte in order to speed up writing and retrieval
	 */
	static long roundUp(long val) {
		val = val == 0 ? Byte.SIZE : val;
		try {
			return (val % Byte.SIZE == 0) ? val : val + (Byte.SIZE - (val % Byte.SIZE));
		}catch (ArithmeticException e){
			return 0;
		}

	}

	/**
	 * Returns the number of lower bits required to encode each element in an
	 * array of size {@code length} with maximum value {@code u}.
	 * 
	 * @param u			the max value in the array
	 * @param length	the length of the array
	 * @return 			the number of lower bits
	 */
	public static int getL(final int u, final int length) {
		return (int) Math.ceil(Math.log(u/(double)length)/Math.log(2));
	}

	/**
	 * Compresses elements of {@code in} into the {@code out} array
	 * 
	 * @param in the array to compress
	 */
	public static void compress(final List<Integer> in, byte [] out, int l, long highBitsOffset) {
		int lowOffset = 0;
		// need to write the first number and save the first high part
		int mask = (1 << l) - 1;						// Example: 0001000 - 1 = 0000111
		int low = in.get(0) & mask;						// I need only the l low bits
		Bits.writeBinary(out, lowOffset, low, l);
		lowOffset += l;
		int high = in.get(0) >>> l;						// keep only the n-l MSBs
		if(high != 0) {
			highBitsOffset += high;			//if first high is different from all 0s, I need to write how many 0s I skipped
		}
		int prev = high;								// save what was the previous high part
		int count = 1;									// how many equals high part I have
		for(int i = 1; i < in.size(); i++) {
			mask = (1 << l) - 1;						// Example: 0001000 - 1 = 0000111
			low = in.get(i) & mask;						// I need only the l low bits
			Bits.writeBinary(out, lowOffset, low, l);
			lowOffset += l;
			high = in.get(i) >>> l;						// keep only the n-l MSBs
			if(high == prev)
				count++;								//update the number of equals high part
			else{
				Bits.writeUnary(out, highBitsOffset, count);
				highBitsOffset += count + 1;						// I write high 1s and one 0
				//if high is not the successor of prev, I need to write n 0s = (high - prev)
				if(high - prev > 1)
					highBitsOffset += (high - prev) - 1;
				prev = high;							//change the new prev
				count = 1;
			}
		}
		// write the last count
		Bits.writeUnary(out, highBitsOffset, count);
	}

	/**
	 * Decompresses elements of {@code in} into a list of Integer
	 *
	 * @param in 			The compressed byte array containing both low and high bits.
	 * @param length 		The number of document IDs to decompress.
	 * @param maxDocId 		The maximum document ID used during compression.
	 * @return 				A list of decompressed document IDs.
	 */
	public static List<Integer> decompress(final byte[] in, final int length, final int maxDocId) {
		List<Integer> docIds = new ArrayList<>();
		long lowBitsOffset = 0;
		final int l = getL(maxDocId,length);
		long highBitsOffset = roundUp(((long) l * length));		// The byte array is formed by lowBits | highBits

		int currentHighBitNumber = 0;							// what is the current HighPart
		int howMany = Bits.readUnary(in, highBitsOffset);		// how many of the same highPart I have
		highBitsOffset += howMany + 1;							// update the offset (howMany 1s + one 0)
		while(howMany == 0){									// the first part must have a cluster with more than 0 elements
			howMany = Bits.readUnary(in, highBitsOffset);
			highBitsOffset += howMany + 1;
			currentHighBitNumber++;
		}
		for (int i = 0; i < length; i++) {
			final int low = Bits.readBinary(in, lowBitsOffset, l);
			docIds.add((currentHighBitNumber << l) | low);
			lowBitsOffset += l;
			howMany--;					// decrease the number of howMany every time I use the same HighBits
			while(docIds.size()<length && howMany == 0){
				howMany = Bits.readUnary(in, highBitsOffset);
				highBitsOffset += howMany + 1;
				currentHighBitNumber++;
			}
		}
		return docIds;
	}
	/**
	 * Returns the number of bytes required to compress an array of size
	 * {@code length} and maximum value {@code u}.
	 * 
	 * @param u			the maximum value in the array to compress
	 * @param length	the size of the array to compress
	 * @return 			the number of required bytes
	 */
	public static int getCompressedSize(final int u, final int length) {
		// In my implementation highBits array starts in a new byte, in the worst case I waste 7 bit
		// but the read/write operation are simpler to implement
		final int l = getL(u, length);
		final long numLowBits = roundUp(l * length);
		final long numHighBits = roundUp(2 * length);			//2nlog2(n)
		return (int) ((numLowBits + numHighBits) / Byte.SIZE);		//how many byte, it's always an integer since the numerator is a multiple of Byte.SIZE
	}

	/**
	 * Decompresses the idx-th element from the compressed array {@code in},
	 * The uncompressed array has size
	 * {@code length} and its elements are encoded using {@code l} lower bits
	 * each.
	 *
	 * @param in		the compressed array
	 * @param length	the size of the uncompressed array
	 * @param idx		the index of the element to decompress
	 * @return the value of the idx-th element
	 */
	public static int get(final byte[] in, final int maxDocId, final int length, final int idx, EliasFanoCache cache) {
		if (idx >= length)
			return Integer.MAX_VALUE;
		int l = getL(maxDocId,length);
		/* I recover values from EliasFanoCache in order to avoid the decompression of the first part of high bit
		 *	numbers if I have already done
		 */
		long highBitsCached = cache.getHighBitsOffset();
		int numberOfDocIdsCached = cache.getNumberOfDocIdsCached();
		int currentHighBitNumberCached = cache.getCurrentHighBitNumber();
		long highBitsOffset = highBitsCached == -1 ? roundUp(l * length) : highBitsCached;

		final int low = Bits.readBinary(in, l * idx, l);		// read the low part (I know che exact position, since I know l)
		int currentHighBitNumber = currentHighBitNumberCached == -1 ? 0 : currentHighBitNumberCached;
		int howMany;									// how many of the same highPart I have
		int numberOfDocIds = numberOfDocIdsCached == -1 ? 0 : numberOfDocIdsCached;
		while(numberOfDocIds < idx + 1){
			howMany = Bits.readUnary(in, highBitsOffset);
			highBitsOffset += howMany + 1;
			numberOfDocIds += howMany;
			if(numberOfDocIds < idx + 1)
				currentHighBitNumber++;
		}
		cache.setCache(highBitsOffset,numberOfDocIds,currentHighBitNumber);
		return (currentHighBitNumber << l) | low;
	}

	// Should I explore the nextGEQ function using the EliasFano characteristics
}
