class DocumentIndex:
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(DocumentIndex,cls).__new__(cls)
        return cls.instance

