# Parser Class

## Overview
The `Parser` class provides functionality to tokenize and parse textual documents. It includes methods for tokenization, stemming, stopwords filtering, and document parsing. The primary purpose is to process text data and extract tokens.

## Execution Flow

### 1. Tokenization (`getTokens` method)
The `getTokens` method tokenizes the input text into a list of strings based on the specified processing steps. The tokenization process involves the following steps:

- **HTML Removal**: Any HTML tags in the input text are removed.
- **Punctuation Removal**: Punctuation marks are replaced with spaces.
- **Lowercasing**: All words are converted to lowercase.
- **Invalid Character Removal**: Words containing invalid UTF-8 characters are excluded.
- **Size and Content Filtering**: Words with a length exceeding a defined threshold (`Lexicon.TERM_DIMENSION`) are filtered out. Additionally, consecutive non-numerical characters in a word are reduced to a maximum of two.

Optionally, if the `parseFlag` is set to `true`, additional processing steps are applied:
- **Stopwords Filtering**: Common stopwords are removed.
- **Stemming**: Words are stemmed using the PorterStemmer algorithm.

### 2. Remove Consecutive Characters (`removeConsecutiveCharacter` method)
The `removeConsecutiveCharacter` method removes consecutive non-numerical characters in a word, reducing them to a maximum of two.

### 3. Stopwords Filtering (`stopwords_filtering` method)
The `stopwords_filtering` method filters out common stopwords from a list of tokens. Stopwords are loaded from an external file (`utils/stopwords.txt`). If the file is not found, the process continues without stopwords filtering.

### 4. Stemming (`stemming` method)
The `stemming` method performs stemming on a list of tokens using the PorterStemmer algorithm.

### 5. Document Parsing (`parseDocument` method)
The `parseDocument` method extracts a document's PID (Product ID) using a regular expression and then parses the remaining text using the `getTokens` method. The result is a `ParsedDocument` object containing the PID and the list of tokens.

## Exception Handling
- If the PID cannot be found in the document (no match for the PID regex), a `PidNotFoundException` is thrown.
- If there are issues with encoding during tokenization, an `UnsupportedEncodingException` is thrown.

## Note
- The `Parser` class assumes that the `Lexicon` class and its `TERM_DIMENSION` constant are defined elsewhere in the codebase.
- Ensure that the `stopwords.txt` file is available in the specified path (`utils/stopwords.txt`) for stopwords filtering.
