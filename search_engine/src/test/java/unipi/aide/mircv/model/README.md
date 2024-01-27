# Test

Here are presents only the tests performed with JUnit. Other tests were not performed with JUnit forr two reason

- Simple functions: just read the code to understand its correctness. These functions mostly call other functions, like an orchestrator.

- Read and write operations and more complex functions: tested by running the code (and the debugger provided by IntelliJ) with different collection sizes, first with collections containing a few documents, then gradually increasing the size until reaching the complete collection.

## BitsTest Class

The `BitsTest` class is designed to test various functionalities related to unary compression. It includes the following test methods:

- **readUnary:** Tests the reading functionality of unary compression.
  
- **writeUnary:** Evaluates the writing functionality of unary compression.

- **readUnaryLimitCase:** Examines limit cases during reading, ensuring robustness in scenarios where bits are not aligned to a byte and a compressed number occupies more than one byte.

## CompressedPostingListTest Class

The `CompressedPostingListTest` class is focused on testing the `CompressedPostingList` class. It includes the following test methods:

- **CompressedPostingListConstructor:** Tests the constructor of `CompressedPostingList`, specifically focusing on the conversion from an uncompressed to a compressed posting list in various situations.

- **nextGEQTest:** Evaluates the `nextGEQ` method of `CompressedPostingList`.

## EliasFanoTest Class

The `EliasFanoTest` class is dedicated to testing the functionalities of the Elias-Fano encoding. It includes the following test methods:

- **compress:** Tests the compression functionality of Elias-Fano.

- **compressLowBitsGreaterThan1Byte:** Evaluates the compression of low-order bits when they exceed one byte.

- **compressHighBitsGreaterThan1Byte:** Examines the compression of high-order bits when they exceed one byte.

- **decompress:** Tests the decompression functionality of Elias-Fano.

- **getTest1, getTest2, getTest3:** Various tests on the `get` method.

- **getL, getCompressedSize** They are tests on auxiliary functions.

## PostingsListsTest Class

The `PostingsListsTest` class is designed to test the functionalities related to posting lists. It includes the following test methods:

- **add Function Test:** Tests the `add` function, which inserts new posting lists associated with a given term.

- **sort Function Test:** Verifies the correct operation of the sorting of the Map implemented within the `PostingsLists` class.

## UnaryCompressorTest Class

The `UnaryCompressorTest` class is focused on testing the functionalities of the unary compressor. It includes the following test methods:

- **getByteSizeInUnary Test:** Tests the `getByteSizeInUnary` function.

- **compress Test:** Evaluates the `compress` function.

- **decompressFrequencies Test:** Tests the `decompressFrequencies` function.

- **get Test:** Includes various tests on the `get` method.
