from collections import defaultdict
from typing import Dict, List

from core.indexing.model.posting import Posting

class InvertedIndex:

    partition = 1
    def __init__(self):
        self._index = defaultdict(list)

    def add_posting(self, term: str, docno: str, frequency: int = None) -> None:
        """Adds a document to the posting list of a term."""
        posting = Posting(docno, frequency)
        self._index[term].append(posting)

    def get_postings(self, term: str) -> List[Posting]:
        """Fetches the posting list for a given term."""
        if self._index.get(term):
            return self._index[term]
        return None

    def get_terms(self) -> List[str]:
        """Returns all unique terms in the index."""
        return self._index.keys()

    def sort(self) -> None:
        self._index = sorted(self._index.items(), key=lambda x: x[1])



    def write_to_file(self, filename: str) -> None:
        with open(filename, 'w') as file:
            for term in self._index.keys():
                postings = self.get_postings(term)
                line = term
                for posting in postings:
                    line = line + ' ' + str(posting.docno) + ':' + str(posting.frequency)
                line = line + '\n'
                file.write(line)