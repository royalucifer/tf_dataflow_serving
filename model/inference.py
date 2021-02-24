import os

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io import BigQuerySource
from apache_beam.pvalue import AsSingleton


class Recommend(beam.DoFn):
    def __init__(self, data_path, tf_version=2):
        self.params = {
            'item_map': None,
            'user_item': None}
        self.data_path = os.path.join(data_path, '{}.pkl')
        self.model_path = os.path.join(data_path, 'model/1')

        if tf_version not in (1, 2):
            raise ValueError('Tensorflow version must be 1 or 2, but you give %d' % tf_version)
        self.tf_version = tf_version

    def setup(self):
        # init model
        import tensorflow as tf
        if self.tf_version == 2:
            # If the loaded object is garbage collected, the variables will be deleted as well.
            # Make sure the loaded object remains in memory
            self.__loaded = tf.saved_model.load(self.model_path)
            self.predictor_fn = self.__loaded.signatures["serving_default"]
        elif self.tf_version == 1:
            self.predictor_fn = tf.contrib.predictor.from_saved_model(
                export_dir=self.model_path)

        # init parameters
        for k, _ in self.params.items():
            self.params[k] = self.__expensive_initialization(k)

    def __generate_recommendations(self, user_x, user_rated, k):
        import numpy as np

        if self.tf_version == 2:
            X = tf.constant(user_x.A, dtype=tf.float32)
            pred_ratings = self.predictor_fn(X)["logits"].numpy()[0]
        elif self.tf_version == 1:
            X = user_x.tocoo()
            pred_ratings = self.predictor_fn({
                'indices': list(zip(X.row, X.col)),
                'values': X.data})['output'][0]

        # find candidate recommended item indexes sorted by predicted rating
        k_r = k + len(user_rated)
        candidate_items = np.argsort(pred_ratings)[-k_r:]

        # remove previously rated items and take top k
        recommended_items = [i for i in candidate_items if i not in user_rated]
        recommended_items = recommended_items[-k:]
        recommended_items.reverse()

        # top news
        top_items = candidate_items[-k:].tolist()
        top_scores = pred_ratings[top_items]
        tops = zip(top_items, top_scores)
        return recommended_items, tops

    def process(self, element, k=24, *args, **kwargs):
        user_idx = element['user_idx']
        user_id = element['user_id']

        # retrieve user reading history
        user_x = self.params['user_item'][user_idx]
        already_rated_idx = set(user_x.tocsr().indices)

        # generate list of recommended article indexes from model
        recommendations, tops = self.__generate_recommendations(user_x, already_rated_idx, k)

        # map article indexes back to article ids
        article = [self.params['item_map'][i] for i in recommendations]

        yield pvalue.TaggedOutput('users_recommendation',
                                  {
                                      'indice': user_idx,
                                      'user_id': user_id,
                                      'item_id': article
                                  })
        for k, v in tops:
            yield (self.params['item_map'][k], v)

    def __expensive_initialization(self, object_key):
        from apache_beam.io.gcp import gcsio
        import pickle

        gcs = gcsio.GcsIO()
        with gcs.open(self.data_path.format(object_key), 'rb') as file:
            if object_key == 'item_map':
                temp = pickle.load(file)
                return dict(zip(range(temp.shape[0]), temp))
            else:
                return pickle.load(file)


class FindTopItem(beam.PTransform):
    def _list_format(self, element):
        _, item_id = map(list, zip(*element))
        return {'item_id': item_id}

    def expand(self, news_to_score):
        top_news = (
            news_to_score
            | 'ItemMean' >> beam.combiners.Mean.PerKey()
            | 'KVSwap' >> beam.KvSwap()
            | 'TopItem' >> beam.combiners.Top.Of(self.n)
            | 'FormatItemList' >> beam.Map(self._list_format))
        return top_news
