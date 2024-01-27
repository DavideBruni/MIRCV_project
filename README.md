# MIRCV_project 📃

## Description 📚
This project is the realization of a search engine for the Multimedia Information Retrieval and Computer Vision course within the Master's program in Artificial Intelligence and Data Engineering at the University of Pisa for the academic year 2023/2024. <br> In this repository, you will find (in almost) all the folders a README.md file containing details about the classes present inside.

## JDK Version ☕
The project has been developed using JDK 19.

## Jar Files 📦
In the project directory, you will find two JAR files:

1. **indexer.jar**: Used for creating the search index.
   - **To Run:** `java -jar indexer.jar path/to/collection parse compression [path/to/log/dir]`
   - **Expected Parameters:** 
      -   `path/to/collection`: str, the path to the collection that contains the data to be indexed. **Pay Attention**: Collection file must be compressed as **.tar.gz** with inside one unique tsv file called `collection.tsv`. If you want to change the name of the tsv file, you have to modify `TSV_FILE_NAME` constant inside the `IndexingMain.java` file
      -   `parse`: boolean, if true stopwords removal and stemming will be performed.
      -   `compress`: boolean, if true, posting lists will be compressed. Document-ids are compressed using Elias-Fano and frequencies are compressed using Unary Compression.
      -   `[path/to/log/dir]` (Optional): The optional path to the directory where log file will be stored. If not provided, error logs will be printed in the stderr.

2. **query_processor.jar**: Used for executing queries on the search engine and for running trec_eval.
   - **To Run:** `java -jar query_processor.jar parse compression score_standard evaluation k`
   - **Expected Parameters:** 
      -   `parse`: boolean, set true if index was created using parse parameter set to true.
      -   `compress`: boolean, set true if index was created using compression parameter set to true.
      -   `score_standard`: str, only BM25 and TFIDF are accepted (if the parameter doesn't match one of the two standard, TFIDF will be used as default).
      -   `evaluation`: boolean, if true, the msmarco-test2019-queries.tsv file will be used and a file valid for trec_evaluation will be generated. If false, a CLI will be shown waiting for a query.
      -   `k`:  is the maximum number of documents that the system returns in response to a query (i.e., the top k documents that best match the query).

## Acknowledgments and Thanks 🙌
Special thanks to [https://raw.githubusercontent.com/stopwords-iso/stopwords-en/master/stopwords-en.txt](https://raw.githubusercontent.com/stopwords-iso/stopwords-en/master/stopwords-en.txt) for providing the stopwords.txt file and [(https://github.com/caarmen)](https://github.com/caarmen/porter-stemmer/tree/master) for the Porter stemmer used in this project.

Feel free to explore, contribute, and enhance the MIRCV_project! 🚀
