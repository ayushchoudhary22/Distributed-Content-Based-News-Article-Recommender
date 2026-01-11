from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[\w']+")  # regex to match words (alphanumeric and apostrophes)

class MRDTM(MRJob):

    def mapper(self, _, line):
        # Split each line of news.tsv by tab into fields
        parts = line.strip().split('\t')
        if len(parts) < 4:
            return  # skip lines that don't have expected number of columns

        doc_id = parts[0]              # news ID (document identifier)
        text = parts[3].lower()        # using the Title field as text (4th column, zero-indexed)

        # Emit each word in the title with key=(word, doc_id) and value=1
        for word in WORD_RE.findall(text):
            yield (word, doc_id), 1

    def reducer(self, key, values):
        # key is (word, doc_id); values is an iterable of 1s
        word, doc_id = key
        total_count = sum(values)

        # Output the word as key, and a tuple of (frequency, doc_id) as value
        yield word, (total_count, doc_id)


if __name__ == '__main__':
    MRDTM.run()
