from dataclasses import dataclass


@dataclass
class LexiconEntry:
    df: int = 0         # document frequency
    idf: int = 0        # inverse document frequency
    # max_idf: int = 0    # max idf value
    # max_tf:  int = 0    # max tf value
