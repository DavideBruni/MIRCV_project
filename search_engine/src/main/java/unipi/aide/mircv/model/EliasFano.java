package unipi.aide.mircv.model;

import java.util.ArrayList;
import java.util.List;

public class EliasFano {


	private EliasFano() {
		
	}
	
	static long roundUp(long val, final long den) {

		val = val == 0 ? den : val;
		return (val % den == 0) ? val : val + (den - (val % den));

	}

	/**
	 * Returns the number of lower bits required to encode each element in an
	 * array of size {@code length} with maximum value {@code u}.
	 * 
	 * @param u
	 *            the max value in the array
	 * @param length
	 *            the length of the array
	 * @return the number of lower bits
	 */
	public static int getL(final int u, final int length) {

		long x = roundUp(u, length) / length;
		return Integer.SIZE - Integer.numberOfLeadingZeros((int) (x - 1));
	}

	/**
	 * Compresses {@code length} elements of {@code in}, from {@code inOffset},
	 * into the {@code out} array, from {@code outOffset}
	 * 
	 * @param in
	 *            the array to compress (MONOTONICALLY INCREASING)
	 * @return the compress values
	 */
	public static void compress(final List<Integer> in, byte [] out, int l, long[] docIdsOffset) {
		int prev = 0;
		for (int i = 0; i < in.size(); i++) {
			int low = Bits.VAL_TO_WRITE[l] & in.get(i);
			Bits.writeBinary(out, docIdsOffset[0], low, l);
			docIdsOffset[0] += l;
			int high = in.get(i) >>> l;
			Bits.writeUnary(out, docIdsOffset[1], high - prev);
			docIdsOffset[1] += (high - prev) + 1;
			prev = high;
		}
	}

	public static List<Integer> decompress(final byte[] in, final int length, final int maxDocId) {

		List<Integer> docIds = new ArrayList<>();
		long lowBitsOffset = 0;
		final int l = getL(maxDocId,length);
		long highBitsOffset = roundUp(lowBitsOffset + (l * length), Byte.SIZE);

		int delta = 0;
		for (int i = 0; i < length; i++) {
			final int low = Bits.readBinary(in, lowBitsOffset, l);
			final int high = Bits.readUnary(in, highBitsOffset);
			delta += high;
			docIds.add((delta << l) | low);
			lowBitsOffset += l;
			highBitsOffset += high + 1;
		}

		return docIds;
	}
	/**
	 * Returns the number of bytes required to compress an array of size
	 * {@code length} and maximum value {@code u}.
	 * 
	 * @param u
	 *            the maximum value in the array to compress
	 * @param length
	 *            the size of the array to compress
	 * @return the number of required bytes
	 */
	public static int getCompressedSize(final int u, final int length) {

		final int l = getL(u, length);
		final long numLowBits = roundUp(l * length, Byte.SIZE);
		final long numHighBits = roundUp(2 * length, Byte.SIZE);
		return (int) ((numLowBits + numHighBits) / Byte.SIZE);
	}


	/**
	 * Decompresses the idx-th element from the compressed array {@code in},
	 * The uncompressed array has size
	 * {@code length} and its elements are encoded using {@code l} lower bits
	 * each.
	 *
	 * @param in
	 *            the compressed array
	 * @param length
	 *            the size of the uncompressed array
	 * @param idx
	 *            the index of the element to decompress
	 * @return the value of the idx-th element
	 */
	public static int[] get(final byte[] in, final int maxDocId, final int length, final int idx, int highBitsOffsetCache, int previous1BitCache) {
		int l = getL(maxDocId,length);
		final long highBitsOffset = roundUp(l * length, Byte.SIZE);
		final int low = Bits.readBinary(in, l * idx, l);
		final int startOffset = (int) (highBitsOffset / Byte.SIZE);

		int _1Bits;
		if (highBitsOffsetCache < 0) {
			highBitsOffsetCache = startOffset;
			previous1BitCache = 0;
			_1Bits = 0;
		}else{
			_1Bits = previous1BitCache;
		}

		while (_1Bits <= idx) {
			previous1BitCache = _1Bits;
			_1Bits += Integer.bitCount(in[highBitsOffsetCache++] & 0xFF);
		}
		highBitsOffsetCache--; // rollback
		int delta = ((highBitsOffsetCache - startOffset) * Byte.SIZE) - previous1BitCache; // delta
		int readFrom = highBitsOffsetCache * Byte.SIZE;
		for (int i = 0; i < (idx + 1) - previous1BitCache; i++) {
			int high = Bits.readUnary(in, readFrom);
			delta += high;
			readFrom += high + 1;
		}

		return new int[]{(delta << l) | low, highBitsOffsetCache, previous1BitCache};
	}


	public static int nextGEQ(final byte[] in, final int length, final int maxDocId, final int val) {

		int l = getL(length,maxDocId);
		final long highBitsOffset = roundUp(l * length, Byte.SIZE);

		final int h = val >>> l;

		final int startOffset = (int) (highBitsOffset / Byte.SIZE);
		int offset = startOffset;
		int prev1Bits = 0;
		int _0Bits = 0;
		int _1Bits = 0;
		while (_0Bits < h && _1Bits < length) {
			prev1Bits = _1Bits;
			int bitCount = Integer.bitCount(in[offset++] & 0xFF);
			_1Bits += bitCount;
			_0Bits += Byte.SIZE - bitCount;
		}

		offset = Math.max(offset - 1, startOffset); //conditional rollback

		int low = Bits.readBinary(in, l * prev1Bits, l);
		int delta = ((offset - startOffset) * Byte.SIZE) - prev1Bits; // delta
		int readFrom = offset * Byte.SIZE;
		int high = Bits.readUnary(in, readFrom);
		delta += high;
		readFrom += high + 1;

		if (((delta << l) | low) >= val) {
			return prev1Bits;
		} else {
			for (int i = prev1Bits + 1; i < length; i++) {
				low = Bits.readBinary(in, l * i, l);
				high = Bits.readUnary(in, readFrom);
				delta += high;
				readFrom += high + 1;
				if (((delta << l) | low) >= val) return i;
			}
		}

		return -1 ;

	}
}
