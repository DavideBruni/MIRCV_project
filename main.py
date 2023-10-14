import argparse
import tarfile
from typing import Tuple

from core.parser import Parser
from model.invertedIndex import InvertedIndex


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
    index = InvertedIndex()
    try:
        with tarfile.open(input_file, "r:gz") as tar:
            for member in tar.getmembers():
                 f = tar.extractfile(member)
                 if f is not None:
                     content = f.read().decode('utf-8')
                     docno, tokens = parser.parse_doc(content,filtering)
                     for term in set(tokens):
                        index.add_posting(term=term, docno=docno, payload=tokens.count(term))
    except Exception as e:
        print("Exception while reading input file")


if __name__ == "__main__":
    main()
