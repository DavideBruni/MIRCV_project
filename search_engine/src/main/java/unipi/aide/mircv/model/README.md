# Model Package
A concise overview of each class is provided; for further details, refer to the comprehensive information available in the javadoc and comments embedded within the code.

## Bits Class
The `Bits` class serves as a utility class for bitwise operations and compression techniques. Key features include:
- Methods for reading and writing binary and unary-encoded values in a byte array.
- A static array for pre-computed masks, essential for efficient binary encoding.

## BlockDescriptor Class
The `BlockDescriptor` class encapsulates information about a block within a posting list. Important attributes include:
- `maxDocId`: The maximum document ID within the block.
- `numberOfPostings`: The count of postings in the block.
- `nextFrequenciesOffset`: The offset to the frequencies array in the next block.
- `indexNextBlockDocIds`: The index of the document IDs in the next block.

This class proves crucial during query processing for efficient traversal and understanding of posting list blocks.

## CollectionStatistics Class
The `CollectionStatistics` class maintains essential statistics about the entire document collection. Attributes are:
- `collectionSize`: The total number of documents in the collection.
- `documentsLen`: The sum of the lengths of all documents in the collection.
- `numberOfTokens`: The size of the lexicon, representing the total number of unique terms.

## CompressedPostingList Class
The `CompressedPostingList` class represents a compressed version of a posting list and implements methods from the abstract parent class `PostingList`. Notable features include:
- `compressedIds`: Byte array storing compressed document IDs and skipping pointers.
- `compressedFrequencies`: Byte array storing compressed frequencies and number of posting in each block.
- Methods for writing and reading from disk.
- Implementation of `next()`, `nextGeq(int docid)`, and `score()` methods for query processing.

## DocumentIndex Class
The `DocumentIndex` class manages the mapping between document numbers and document IDs. Key attributes include:
- `index`: A private static array list containing the mapping of document IDs to document numbers.

This class hides the implementation details of the document index and provides a simple mapping from document numbers to document IDs.
If mapping between docId and pid must change, just change the implementation of this class.

## EliasFano
The `EliasFano` class offers an implementation of the Elias-Fano compression technique. Key functionalities include:
- The `compress` method separates high and low bits. For each group of low bits, it updates the cluster counter with the same high bits. When the cluster changes, it writes the unary value of the counter for the last cluster(s), considering empty clusters. The compressed list is written in the `out` array by concatenating low bits | high bits. If the last byte of low bits has remaining unused bits, they are wasted, as high bits always start in a new byte. However, in the worst case, only 7 bits are wasted.

- The `decompress` method reads the number of elements with the same prefix (say, n numbers), then reads n low bits to retrieve the original uncompressed number. It increments the prefix and repeats until all bits are read.

- The `get` method retrieves the value at the index `idx` of the compressed list. Low bits are obtained by simply reading the `idx * l` input array index. High bits, on the other hand, are retrieved by summing up unary parts until the `idx` value is reached. To expedite reading, an `EliasFanoCache` instance is used, where the last high bits offset, the number of docIds, and the last high part are saved.

## EliasFanoCache
Utility class to store caching information for Elias-Fano compression. This class is designed to store and retrieve caching information such as high bits offset, the number of cached document IDs, and the current high bit number. It is used to speed up write and read operations during Elias-Fano compression.

## InvertedIndex
 The `InvertedIndex` class, is responsible for managing inverted indices. It includes a method named `SPIMI` designed to create partial indices. The workflow of the class involves reading the collection line by line, parsing each line, and creating or updating posting lists for each token encountered. Once the entire collection has been processed, the `createInvertedIndex` method calls the `Merge` method to consolidate the partial indices into a single inverted index.

## Lexicon
The `Lexicon`class provides functionalities for managing and compressing lexicon entries. It implements the Singleton pattern, ensuring a single instance is used throughout the application. Key features include:
- **Merge Operations**: Several functions are dedicated to handling merge operations of partial lexicons. It has method for reading tokens from partial lexicon streams.

- **Disk Operations**: Lexicon entries can be retrieved from and written to disk. The `writeOnDisk(boolean is_merged, boolean debug)` method supports writing either as a merged lexicon or individual partition files; instead the `getEntryFromDisk(String token,boolean is_merged)` method performs a binary search to retrieve the lexicon entry socciated to the token.

## LexiconEntry
The `Lexicon` class located in the `unipi.aide.mircv.model` package, provides functionalities for managing and compressing lexicon entries. It implements the Singleton pattern, ensuring a single instance is used throughout the application. Key features include:
- **Attributes:** The class stores information such as document frequency, document ID offset, frequency offset, and upper bounds for scoring (BM25 and TFIDF).

- **Writing to Disk:** The `writeOnDisk` method efficiently writes lexicon entries to disk, considering whether the lexicon is merged or not.

## ParsedDocument
The `ParsedDocument` class represents a parsed document with a unique identifier (`pid`) and a list of tokens extracted from the document.

## PostingList
The `PostingList` class, an abstract class, serves as the base class for different types of posting lists used in the inverted index.
Attributes in common between Compressed and Uncompressed list are: 
- `blockDescriptor`: Block descriptor for the posting list.
- `lexiconEntry`: Lexicon entry associated with the posting list.
- `currentIndexPostings`: Index of the current posting in the list (used during query processing).

## PostingLists
The `PostingLists` class manages a collection of posting lists for different tokens in an inverted index using a map where each token is associated with its corresponding posting list.
Methods are:
- `add`: Adds a posting to the inverted index of a given token. If the token does not exist in the postings, a new posting list is created.
- `sort`: Sorts the posting lists based on their tokens.
- `writeOnDisk`: Writes posting lists to disk, either compressed or uncompressed.

## UnaryCompressor
The `UnaryCompressor` class provides methods for compressing and decompressing lists of frequencies using unary encoding.
Methods are:
- `getByteSizeInUnary`: Calculates the size, in bytes, needed to represent a list of frequencies in unary encoding.
   - `compress`: Compresses a list of frequencies using unary encoding.
   - `decompressFrequencies`: Decompresses frequencies from a byte array using unary encoding.
   - `get`: Reads the unary-encoded frequencies from the given compressed byte array.
  
## UncompressedPostingList
The `UncompressedPostingList` class, an extension of the `PostingList` abstract class, represents an uncompressed posting list.
Main attributes are:
- `docIds`: List of document IDs (Integer) in the posting list.
- `frequencies`: List of frequencies (Integer) corresponding to the document IDs.
