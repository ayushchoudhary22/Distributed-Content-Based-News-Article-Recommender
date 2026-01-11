from mrjob.job import MRJob
from mrjob.step import MRStep
import ast
import math
import heapq
from collections import defaultdict

DEFAULT_TOP_N = 5  # default top N recommendations

class MRRecommend(MRJob):

    def configure_args(self):
        super(MRRecommend, self).configure_args()
        self.add_file_arg('--dtm', help='Path to dtm_output.txt or article_profiles.txt')
        self.add_passthru_arg('--top-n', type=int, default=DEFAULT_TOP_N, help='Top N recommendations per user')

    def mapper_init(self):
        """
        Load article profiles and precompute magnitudes
        """
        self.article_profiles = defaultdict(dict)
        try:
            with open(self.options.dtm, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    doc_id, profile_str = line.split('\t', 1)
                    try:
                        profile = ast.literal_eval(profile_str)
                        if isinstance(profile, list):
                            profile = dict(profile)
                        self.article_profiles[doc_id] = profile
                    except:
                        continue
        except:
            pass

        # Precompute magnitudes of article vectors
        self.article_magnitudes = {}
        for doc_id, profile in self.article_profiles.items():
            mag = math.sqrt(sum(freq ** 2 for freq in profile.values()))
            self.article_magnitudes[doc_id] = mag

        self.top_n = int(self.options.top_n)

    def mapper(self, _, line):
        """
        For each user, compute cosine similarity with all articles
        """
        parts = line.strip().split('\t', 1)
        if len(parts) != 2:
            return
        user_id, profile_str = parts
        try:
            user_profile = ast.literal_eval(profile_str)
            if isinstance(user_profile, list):
                user_profile = dict(user_profile)
        except:
            return

        if not user_profile:
            return

        user_mag = math.sqrt(sum(v ** 2 for v in user_profile.values()))
        if user_mag == 0:
            return

        for doc_id, article_profile in self.article_profiles.items():
            article_mag = self.article_magnitudes.get(doc_id, 0)
            if article_mag == 0:
                continue
            # Compute dot product
            dot = sum(user_profile.get(word, 0) * freq for word, freq in article_profile.items())
            if dot > 0:
                cosine_sim = dot / (user_mag * article_mag)
                yield user_id, (cosine_sim, doc_id)

    def reducer_find_top(self, user_id, values):
        """
        Select top-N articles per user
        """
        top_articles = heapq.nlargest(self.top_n, values, key=lambda x: x[0])
        yield user_id, [doc for _, doc in top_articles]

    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init, mapper=self.mapper, reducer=self.reducer_find_top)
        ]


if __name__ == '__main__':
    MRRecommend.run()

