from core.indexing.model.lexiconEntry import LexiconEntry


class Lexicon:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Lexicon, cls).__new__(cls)
            cls._instance._lexicon = {}
        return cls._instance

    def add_term(self, term: str, lexiconEntry: LexiconEntry) -> None:
        self._lexicon[term] = lexiconEntry

    def get_term(self, term: str) -> LexiconEntry:
        return self._lexicon.get(term, None)
