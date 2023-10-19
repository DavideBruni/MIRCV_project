from dataclasses import dataclass


@dataclass
class Posting:
    docno: str
    frequency: int