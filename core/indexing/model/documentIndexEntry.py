from dataclasses import dataclass


@dataclass
class DocumentIndexEntry:
    pid:int
    doc_id:int
    doc_len:int
    mem_offset:int