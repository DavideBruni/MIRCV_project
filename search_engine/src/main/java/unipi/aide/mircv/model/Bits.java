package unipi.aide.mircv.model;

/**
 * Utility class for bitwise operations and compression techniques.
 * This class provides methods for writing and reading binary and unary-encoded values
 * in a byte array. It also includes a static array for pre-computed masks used during
 * binary encoding. The class is designed to support compression techniques.
 */
public final class Bits {

	private Bits() {
		// Private constructor to prevent instantiation of the utility class.
	}

	static final int[] VAL_TO_WRITE;

	static {
		/*I prefer to use an array of 33 values in order to avoid the calculation of the mask each time I have to compress
			I need 33 values since I have to consider all possibilities, from 0 to 32 bits to be written*/
		VAL_TO_WRITE = new int[Integer.SIZE + 1];
		for (int i = 0; i <= Integer.SIZE; i++)
			VAL_TO_WRITE[i] = 0xFFFFFFFF >>> (Integer.SIZE - i);
	}

	static void writeBinary(final byte[] in, long bitOffset, int val, int numbits) {
		while (numbits > 0) {		//while I have bit to write
			final int byteOffset = (int) (bitOffset / Byte.SIZE);	//in which byte I have to write
			final int bitPosition = (int) (bitOffset % Byte.SIZE);	//at what position of the byte (which bit)
			final int availableSpace = Byte.SIZE - bitPosition;
			final int bitsToWrite = Math.min(numbits, availableSpace);	//The number of bit I can write starting from that position
			final int shift = numbits - availableSpace;

			if (availableSpace < numbits) {				//i.e. shift > 0
				in[byteOffset] |= val >>> shift;		//I write only the first k - shift bit
				val &= VAL_TO_WRITE[shift];				//I reset the k-shift bit I wrote
			} else {
				in[byteOffset] |= val << -shift;		//shift is negative, so I need a positive number, need also to write in the correct position so I use <<
				val &= VAL_TO_WRITE[-shift];			//I reset the k-shift bit I wrote
			}
			bitOffset += bitsToWrite;					// update position and counter
			numbits -= bitsToWrite;
		}
	}

	static int readBinary(final byte[] in, long bitOffset, int numbits) {
		int val = 0;
		while (numbits > 0) {
			final int byteOffset = (int) (bitOffset / Byte.SIZE);
			final int bitPosition = (int) (bitOffset % Byte.SIZE);
			final int availableSpace = Byte.SIZE - bitPosition;
			final int bitsToRead = Math.min(numbits, availableSpace);
			// if I need more than one iteration, I have to perform left shift in order to not overwrite the bits already read
			val <<= bitsToRead;
			int mask = 0xFF >>> bitPosition;				// avoid the first bits if necessary
			int read = mask & in[byteOffset];
			final int shift = availableSpace - numbits;		// if > 0 means that I read more than necessary
			if (shift > 0) {								// for this reason I need right shift
				read >>>= shift;
			}
			val |= read;									//writing the bits read
			bitOffset += bitsToRead;
			numbits -= bitsToRead;
		}

		return val;
	}

	static long writeUnary(final byte[] in, long bitOffset, int val) {
		while (val > 0) {
			final int byteOffset = (int) (bitOffset / Byte.SIZE);
			final int bitPosition = (int) (bitOffset % Byte.SIZE);
			final int availableSpace = Byte.SIZE - bitPosition;
			final int bitToWrite = Math.min(val, availableSpace);
			final int positionLast1 = Byte.SIZE - bitToWrite; //I want the 1s in leading position: how many 0s I have at the end
			final int ones = 0b11111111 & (0b11111111 << positionLast1);	//shift in order to have bitToWrite 1s in leading position

			// shift the number to the correct starting position and set the bits
			in[byteOffset] |= (byte) (ones >>> bitPosition) ;
			bitOffset += bitToWrite;		//updating position and remaining bits to write
			val -= bitToWrite;
		}
		return bitOffset;
	}

	static int readUnary(final byte[] in, long bitOffset) {
		int val = 0;
		int byteOffset = (int) (bitOffset / Byte.SIZE);
		final int bitPosition = (int) (bitOffset % Byte.SIZE);
		byte x = (byte) (in[byteOffset] & (byte) (0b11111111 >>> bitPosition));    //I have to discard the first bitPositionByte
		x = (byte) ~x;						// use the negation
		x = (byte) (x << bitPosition);		//align the start of the unary compression with the start of the byte

		if (x == 0){						// if equal to 0, the compressed number stay on More than one Byte
			val += Byte.SIZE - bitPosition;			//count the number of 1, i.e. ByteSize - bitPosition
			for (;(~in[byteOffset+1]) == 0; byteOffset++) 	//if ~x == 0 --> x is equal to a byte with all 1
				val+= Byte.SIZE;							// I need to add 8
			x = (byte) ~in[byteOffset + 1];					// the remaining part is equal to the number of leading 0
		}									//else, the uncompressed number is equal to the number of leading 0 of ~X
		int partialRes = (Byte.SIZE - (Integer.SIZE - Integer.numberOfLeadingZeros(x)));
		partialRes = Math.max(partialRes,0);
		return val + partialRes;
		/* since number of leading 0 is a function of Integer Class, I have to use this trick to count the number of leading 0s
			in a single byte: (Byte.SIZE - (Integer.SIZE - Integer.numberOfLeadingZeros(x)), i.e.:
			count the number of bits after the leading 0s, then subtract this number from 8 (bits), then sum with val (if number was compress on a single
			byte, val was equal to 0)
		*/
	}
}