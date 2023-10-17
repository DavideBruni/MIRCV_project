import argparse
import gc
import tarfile
from io import TextIOWrapper
from typing import Tuple
import psutil



from core.parser import Parser
from core.indexing.model.invertedIndex import InvertedIndex


def config() -> Tuple[str, bool]:
    """ Returns the input arguments """
    parser = argparse.ArgumentParser(description='Normalizer')
    parser.add_argument('-i', '--input', type=str, help='collection input file')
    parser.add_argument('-f', '--filtering', type=bool, help='true to perform stemming and stopwords filtering, false otherwise')

    args = parser.parse_args()
    return args[1],args[2]

def main():
    parser = Parser()

    input_file, filtering = config()
    tsv_file_name = "collection.tsv"        #TODO sposta costanti in un file delle costanti
    total_memory = psutil.virtual_memory().total  # Get total memory
    memory_threshold = total_memory * 0.2  # Keep the 20% of memory free
    allDocProcessed = False

    # TODO da qualche parte
    # TODO //create new document index entry and add it to file
    #                     DocumentIndexEntry docIndexEntry = new DocumentIndexEntry(
    #                             processedDocument.getPid(),
    #                             docid,
    #                             documentLength
    #                     );

    # TODO FARE IN MODO DI salvarsi le collection statistic in un file : docslen e collection size

    try:
        with tarfile.open(input_file, "r:gz") as tar:
            with tar.extractfile(tsv_file_name) as tsv_file:
                while not allDocProcessed:
                    tsv_text = TextIOWrapper(tsv_file, encoding='utf-8')
                    line = tsv_text.readline()
                    index = InvertedIndex()             # new index object
                    while psutil.virtual_memory().available > memory_threshold:  # until the available memory is over the treshold
                        if line is None:        # read all the lines
                            allDocProcessed = True
                            break
                        if line == '':          # empty line, process the next one
                            line = tsv_text.readline()
                            continue
                        docno, tokens = parser.parse_doc(line,filtering)
                        for term in set(tokens):
                           index.add_posting(term=term, docno=docno, payload=tokens.count(term))
                        line = tsv_text.readline()
                    index.sort()
                    # TODO write to disk
                    del index                           # explicitly delete index from memory
                    gc.collect()                        # invoke garbage collector
        #TODO merge
    except Exception as e:
        print("Exception while reading input file")


if __name__ == "__main__":
    main()
