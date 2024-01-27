# 1. QueryProcessorMain Class

## Overview
The `QueryProcessorMain` class serves as the entry point for executing a query processor. It handles the initialization of necessary structures, input parameter parsing, and the main loop for user interaction or batch evaluation. The core functionality involves processing queries, retrieving relevant documents, and displaying results.

## Execution Flow

### 1. Initialization
- **Input Parameters**: The class expects arguments representing various configuration parameters.
  - `parse document`: Boolean indicating whether to parse documents.
  - `compress index`: Boolean indicating whether to use compressed index structures.
  - `score standard`: String specifying the scoring standard, either "BM25" or "TFIDF".
  - `is TREC eval`: Boolean indicating whether to perform TREC evaluation.
  - `k, how many results show`: Integer specifying the number of results to display.

- **Configuration Setup**: Paths, compression settings, collection statistics, and other configurations are set up based on the input parameters.
- **Lexicon Instantiation**: An empty vocabulary (`Lexicon`) is instantiated to keep in memory LexiconEntry once read.
- **Collection Statistics Loading**: Collection statistics, including document lengths, are read from disk.
- **Document Index Loading (if BM25)**: If the scoring standard is "BM25", the document index is loaded from disk.

### 2. Query Processing Loop
- **User Interaction (GUI) or Batch Evaluation**: Based on the TREC evaluation flag, the program either enters a loop for user interaction or performs batch evaluation using TREC queries.

- **Query Input (User Interaction)**: For user interaction, the program continuously prompts the user to input queries. Queries can be conjunctive (prefixed with '+') or disjunctive.

- **Query Parsing**: The entered query is tokenized using the `Parser.getTokens` method, and the result is a list of parsed query terms.

- **Query Handling**: The core of the query processing is done in the `queryHandler` method, which retrieves posting lists for parsed query terms, sorts them based on term upper bounds, and applies the MaxScore algorithm. The result is a priority queue of document scores (`DocScorePair` objects).

- **Result Display (User Interaction)**: The program prints the top-k results along with their scores and processing time.

### 3. Evaluation (Batch Mode)
- If TREC evaluation is specified, the `evaluation` method is invoked.
- TREC queries are read from the "msmarco-test2020-queries.tsv" file, and results are written to an output file.

## Notes
- Ensure that necessary data files, such as the "msmarco-test2020-queries.tsv" file, are available.

## Exception Handling
- The program may throw runtime exceptions in case of errors during file operations or processing.

# 2. Scorer Class

## Overview
The `Scorer` class provides methods for scoring documents based on the BM25 and TF-IDF ranking algorithms. Additionally, it implements the MAX-SCORE algorithm for efficient conjunctive or disjunctive query processing.

## BM25 and TF-IDF Scoring

### 1. BM25 Score Calculation (`BM25_singleTermDocumentScore`)
- Calculates the BM25 score for a single term in a document.
- Uses term frequency (`tf`), document identifier (`docId`), and document frequency (`df`).
- Handles document length normalization and IDF (Inverse Document Frequency) calculation.

### 2. TF-IDF Score Calculation (`TFIDF_singleTermDocumentScore`)
- Calculates the TF-IDF score for a single term in a document.
- Uses term frequency (`tf`) and document frequency (`df`).
- Applies the standard TF-IDF formula.

## Term Upper Bounds Calculation (`calculateTermUpperBounds`)
- Computes the upper bounds of BM25 and TF-IDF scores for a term in a collection of documents.
- Utilizes an uncompressed posting list and IDF.

## MAX-SCORE Query Processing (`maxScore`)
- Manages a priority queue of `DocScorePair` objects representing documents with the highest scores.
- Supports both conjunctive and disjunctive query processing.
- Utilizes upper bounds to optimize the query processing.
- The result is a priority queue sorted by descending scores.

## Utility Functions

### 1. Minimum Document ID (`minimumDocid`)
- Finds and returns the minimum document ID among a set of posting lists.
- Used in the MAX-SCORE algorithm.

## DocScorePair Class
- A static inner class representing a document-score pair.
- Implements Comparable for sorting based on scores.

## Configuration Parameters
- The class utilizes constants for normalization parameters (`NORMALIZATION_PARAMETER_B`, `NORMALIZATION_OPPOSITE_B`, `NORMALIZATION_PARAMETER_K1`).

## Exception Handling
- Handles potential exceptions such as `DocumentNotFoundException` and `ArithmeticException` during scoring calculations.
- Uses logging (`CustomLogger`) for error reporting.
