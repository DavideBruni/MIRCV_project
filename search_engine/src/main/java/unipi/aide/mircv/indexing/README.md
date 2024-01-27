# IndexingMain class execution flow

1. **Argument Validation:** The main method starts by validating the input arguments. If the required parameters are not provided, an error message is displayed, and the program exits.

2. **Logger Configuration (Optional):** If a log directory path is provided as an argument, the logger is configured to write log files to that directory.

3. **Setting Compression Flag:** The compression flag is set based on the provided input. This flag determines whether the index should be compressed.

4. **Reading from Compressed File:** The program reads the contents of the tar.gz file, automatically handling file closure.

5. **Inverted Index Creation:** For each entry in the compressed file, the program searches for a file with the name "collection.tsv" (supposedly the only file present). If found, it triggers the creation of the inverted index using the `InvertedIndex.createInvertedIndex` method.

6. **Logging Duration and Collection Statistics:** The program logs the time taken to create the index and prints the collection statistics, including information about the indexed documents.

## Command Line Example

## Notes
The class utilizes the Apache Commons Compress library for handling tar.gz files.

**TarArchiveInputStream (`tarIn`):**
   - `new GzipCompressorInputStream(fis)` wraps the `FileInputStream` (`fis`) with a `GzipCompressorInputStream`, which decompresses the Gzip-compressed data.
   - `new TarArchiveInputStream(...)` then wraps the decompressed stream with a `TarArchiveInputStream`. This class is part of the Apache Commons Compress library and allows reading entries (files) from a tar archive.
