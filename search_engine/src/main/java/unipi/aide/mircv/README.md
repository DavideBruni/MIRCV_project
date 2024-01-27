# Main

## 1. Configuration

The `Configuration` directory contains the `Configuration.java` class. This class encapsulates various configuration parameters essential for the proper functioning of the Search Engine. It includes:

- **Several File Paths:** Specifies the paths where files will be written and read once the inverted index creation is completed.

- **Block Size Threshold:** Defines the threshold for the size of a block. If a posting list exceeds this threshold, it needs to be divided into blocks.

- **Compressed Flag:** A flag indicating whether compression is enabled.

- **Query Processing Variables:** Variables used during query processing, such as `score_Standard` and the number of results to return.

## 2. Exceptions

The `Exceptions` directory contains all custom exceptions utilized within the Search Engine code.

## 3. Helpers

The `Helpers` directory contains the `StreamHelpers.java` class, which provides utility methods for creating folders and deleting files within folders or entire folders.

## 4. Indexing

The `Indexing` directory contains the main class executed by `indexer.jar`. This class orchestrates the indexing process. Take a look at the README file present inside this folder.

## 5. Log

The `Log` directory hosts the `CustomLogger` class.

## 6. Model

The `Model` directory contains all classes where the application's logic is managed. This includes classes responsible for the core functionalities of the Search Engine. Take a look at the README file present inside this folder.

## 7. Parsing

The `Parsing` directory contains the `Parser.java` class. This class houses methods for document pre-processing, a crucial step in the Search Engine's functionality. Take a look at the README file present inside this folder.

## 8. Query Processor

The `QueryProcessor` directory includes the `QueryProcessorMain.java` (the main class executed by `query_processor.jar`) and the `Scorer.java` class. The latter contains methods for calculating term upper bounds and the implementation of the maxscorer algorithm. Take a look at the README file present inside this folder.
