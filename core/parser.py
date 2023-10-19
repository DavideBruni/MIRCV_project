import re
import string
from typing import Tuple, List

from nltk.stem import PorterStemmer
from nltk.corpus import stopwords
class Parser:

    def tokenize(self,text:str) -> Tuple[List[str],str]:
        """Returns a sequence of terms and the document pid given an input text."""
        pattern = r'\d+\t'
        match = re.search(pattern, text)
        pid = match.group(0).replace('\t','') if match else None


        text = re.sub(pattern, '', text, count=1)       # removes the pid from the document
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

    def parse_doc(self,document:str,filtering:bool=False) -> Tuple[str,List[str]]:
        """ Returns [a list of tokens given and ???] an input document,
            if filtering is true stemming and stopwords filtering are performed too """

        def remove_invalid_characters(text):
            # Utilizza una regex per rimuovere i byte non validi
            cleaned_text = re.sub(r'[^\x00-\x7F]+', '', text)
            return cleaned_text

        tokens = []
        pid = None
        try:
            tokens,pid = self.tokenize(document)
            tokens = [remove_invalid_characters(token) for token in tokens]
            if filtering:
                tokens = self.stemmer(self.stopwords_filtering(tokens,set(stopwords.words('english'))))
                tokens = [token for token in tokens if len(token) > 1]
        except Exception as e:
            print('Error')
        finally:
            return pid, tokens


