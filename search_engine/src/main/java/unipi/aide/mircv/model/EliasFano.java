/*
 * Copyright 2016-2018 Matteo Catena
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package unipi.aide.mircv.model;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * A simpl(istic) implementation of the EliasFano compression technique. This
 * class compresses array of MONOTONICALLY INCREASING integers into (smaller)
 * arrays of bytes. It permits to uncompress an arbitrary element for the
 * compressed data, without decompressing the whole array. Similarly, it permits
 * to find the index of the first element in the compressed data, greater or
 * equal to a given value, without decompressing the whole array.
 * 
 * @author Matteo Catena
 *
 */
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
	public static int compress(final List<Integer> in, byte [] out, int l, long[] docIdsOffset, int prev) {
		for (int i = 0; i < in.size(); i++) {
			int low = Bits.VAL_TO_WRITE[l] & in.get(i);
			Bits.writeBinary(out, docIdsOffset[0], low, l);
			docIdsOffset[0] += l;
			int high = in.get(i) >>> l;
			Bits.writeUnary(out, docIdsOffset[1], high - prev);
			docIdsOffset[1] += (high - prev) + 1;
			prev = high;
		}
		return prev;
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

	public static int writeToDisk(byte[] compressedDocIds, FileChannel docStream) {
		ByteBuffer buffer = ByteBuffer.allocateDirect(compressedDocIds.length);
		buffer.put(compressedDocIds);
		buffer.flip();
		int written = 0;
		try {
			written = docStream.write(buffer);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return written;
	}
}
