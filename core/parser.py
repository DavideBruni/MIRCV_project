import re
import string
from typing import Tuple, List, Any

import nltk
from nltk.stem import PorterStemmer
from nltk.corpus import stopwords
class Parser:

    def tokenize(self,text:str) -> Tuple[List[str],str]:
        """Returns a sequence of terms and the document pid given an input text."""
        pattern = r'<pid>(.*?)\t'
        match = re.search(pattern, text)
        pid = match.group(1) if match else None


        re_html = re.compile("<[^>]+>")
        text = re_html.sub(" ", text)
        for c in string.punctuation:        # Replace punctuation marks (including hyphens) with spaces.
            text = text.replace(c, " ")
        return text.lower().split(), pid

    def stemmer(self,tokens:List[str]) -> List[str]:
        """Perform PorterStemmer stemming filter on tokens given in input."""
        ps = PorterStemmer()
        return [ps.stem(token) for token in tokens]

    def stopwords_filtering(self,tokens:List[str],stop_words:List[str]) -> List[str]:
        """Removes stopwords from a sequence of tokens."""
        return [token for token in tokens if token not in stop_words]

    def parse_doc(self,document:str,filtering:bool=False) -> Tuple[int,List[str]]:
        """ Returns [a list of tokens given and ???] an input document,
            if filtering is true stemming and stopwords filtering are performed too """
        tokens = []
        pid = None
        try:
            tokens,pid = self.tokenize(document)
            if filtering:
                tokens = self.stemmer(self.stopwords_filtering(tokens,set(stopwords.words('english'))))
        except Exception as e:
            print('Error')
        finally:
            return pid, tokens


