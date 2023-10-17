from dataclasses import dataclass


@dataclass
class LexiconEntry:
    term: str        # term
    df: int          # document frequency
    idf: int         # inverse document frequency
    max_idf: int     # max idf value
    max_tf:  int     # max tf value
