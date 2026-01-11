from mrjob.job import MRJob
import ast
from collections import defaultdict

class UserProfileGeneration(MRJob):

    def configure_args(self):
        super(UserProfileGeneration, self).configure_args()
        # Define an external file argument for the DTM output
        self.add_file_arg('--dtm', help='Path to dtm_output.txt')

    def mapper_init(self):
        # Load the document-term frequencies into a dictionary for quick lookup.
        # self.options.dtm is the local path of the distributed DTM file
        self.doc_word_freq = defaultdict(dict)

        with open(self.options.dtm, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                # Each line in dtm_output is "word<TAB>(freq, doc_id)"
                try:
                    word, val = line.split('\t')
                    freq, doc_id = ast.literal_eval(val)
                except Exception:
                    continue  # skip malformed lines

                # Remove any extraneous quotes (mrjob may output strings with quotes)
                doc_id = doc_id.strip('"')
                word = word.strip('"')

                # Store the frequency in the dictionary
                self.doc_word_freq[doc_id][word] = int(freq)

    def mapper(self, _, line):
        # Input format: user_id<TAB>[doc1, doc2, ...]
        parts = line.strip().split('\t', 1)
        if len(parts) != 2:
            return  # skip bad lines

        user_id, doc_list_str = parts

        # Parse the string representation of the doc list into an actual Python list
        try:
            doc_list = ast.literal_eval(doc_list_str)
        except Exception:
            return  # skip if parsing fails

        # Aggregate word frequencies for this user's profile
        user_profile = defaultdict(int)
        for doc_id in doc_list:
            if doc_id in self.doc_word_freq:
                for word, freq in self.doc_word_freq[doc_id].items():
                    user_profile[word] += freq

        # Emit the user_id and the profile dict (word->freq)
        yield user_id, dict(user_profile)


if __name__ == '__main__':
    UserProfileGeneration.run()
