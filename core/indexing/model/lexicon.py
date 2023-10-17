class Lexicon:
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(Lexicon,cls).__new__(cls)
        return cls.instance
