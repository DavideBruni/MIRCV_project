import tarfile
from io import TextIOWrapper

from core.parser import Parser

# Test for Parser component
def main():
    parser = Parser()
    tsv_file_name = "collection.tsv"

    # Expected tokens for each line of the TSV file
    expected_tokens = [['presenc', 'commun', 'amid', 'scientif', 'mind', 'equal', 'import', 'success', 'manhattan', 'project', 'scientif', 'intellect', 'cloud', 'hang', 'impress', 'achiev', 'atom', 'research', 'engin', 'success', 'truli', 'meant', 'hundr', 'thousand', 'innoc', 'live', 'obliter'],
['manhattan', 'project', 'atom', 'bomb', 'help', 'bring', 'end', 'world', 'war', 'ii', 'legaci', 'peac', 'use', 'atom', 'energi', 'continu', 'impact', 'histori', 'scienc'],
['essay', 'manhattan', 'project', 'manhattan', 'project', 'manhattan', 'project', 'see', 'make', 'atom', 'bomb', 'possibl', 'success', 'project', 'would', 'forev', 'chang', 'world', 'forev', 'make', 'known', 'someth', 'power', 'manmad'],
['manhattan', 'project', 'name', 'project', 'conduct', 'world', 'war', 'ii', 'develop', 'first', 'atom', 'bomb', 'refer', 'specif', 'period', 'project', '194', '1946', 'control', 'armi', 'corp', 'engin', 'administr', 'gener', 'lesli', 'grove']
]

    try:
        with tarfile.open('../test_collection.tar.gz', "r:gz") as tar:
            with tar.extractfile(tsv_file_name) as tsv_file:
                tsv_text = TextIOWrapper(tsv_file, encoding='utf-8')
                line = tsv_text.readline()
                i = 0
                while line:
                    docno, tokens = parser.parse_doc(line,True)
                    assert tokens == expected_tokens[i]
                    assert docno == str(i)
                    line = tsv_text.readline()
                    i = i + 1
        print("Parser test passed")
    except Exception as e:
        print("Parser test not passed")

if __name__ == "__main__":
    main()
