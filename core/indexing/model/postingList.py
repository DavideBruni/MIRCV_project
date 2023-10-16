from collections import defaultdict
from typing import Dict, List

from core.indexing.model.posting import Posting

class PostingList:

    partition = 1
    def __init__(self):
        self._index = defaultdict(list)
        self.lexicon: Dict[List[str], int, int, int] = {}       #TODO fare una classe a parte! Salvare df, idf

    def add_posting(self, term: str, docno: str, payload: int = None) -> None:
        """Adds a document to the posting list of a term."""
        if not self.document_table.get(docno):
            self.document_table[docno] = self.last_doc_id
        posting = Posting(docno, payload)
        self._index[term].append(posting)

    def get_postings(self, term: str) -> List[Posting]:
        """Fetches the posting list for a given term."""
        if self._index.get(term):
            return self._index[term]
        return None

    def get_terms(self) -> List[str]:
        """Returns all unique terms in the index."""
        return self._index.keys()

    def write_to_file(self, filename: str) -> None:
        with open(filename, 'w') as file:
            for term in self._index.keys():
                postings = self.get_postings(term)
                line = term
                for posting in postings:
                    line = line + ' ' + str(posting.doc_id) + ':' + str(posting.payload)
                line = line + '\n'
                file.write(line)