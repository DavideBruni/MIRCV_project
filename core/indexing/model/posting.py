from dataclasses import dataclass


@dataclass
class Posting:
    doc_id: int
    payload: int