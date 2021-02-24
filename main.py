import os
import logging
import argparse
from datetime import datetime, timedelta

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigtableio import WriteToBigTable

from model.inference import Recommend, FindTopItems
from model.jsonio import WriteToJson
from model.fromat import CreateEntities, CreateDirectRow

TIMESTAMP = datetime.utcnow().replace(microsecond=0)
TS_SUFFIX = (TIMESTAMP + timedelta(hours=8)).strftime('%Y%m%d-%H%M%S')


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_path', type=str, required=True)
    parser.add_argument('--destination', type=str, required=True)
    parser.add_argument('--project_id', type=str, required=True)
    parser.add_argument('--instance_id', type=str, required=True)
    parser.add_argument('--table_id', type=str, required=True)
    parser.add_argument('--k', type=str, required=True)
    known_args, pipeline_args = parser.parse_known_args(argv)

    # file path
    data_path = known_args.input_path
    user_map_path = os.path.join(data_path, 'user_map.avro')

    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        news_score, users_recommendation = (
            p
            | 'Initialize' >> beam.io.ReadFromAvro(user_map_path)
            | 'Recommend' >> beam.ParDo(Recommend(data_path), k=known_args.k).
            with_outputs('users_recommendation', main='news_score'))

        (news_score
        | 'FindTopNews' >> FindTopItems()
        | 'Write' >> WriteToJson(
            file_path_prefix=known_args.destination,
            file_name_suffix='-{}.json'.format(TS_SUFFIX),
            shard_name_template=''))

        (users_recommendation
         | 'CreateDirectRow' >> beam.ParDo(CreateDirectRow(), TIMESTAMP)
         | 'WriteToBigTable' >> WriteToBigTable(
            project_id=known_args.project_id,
            instance_id=known_args.instance_id,
            table_id=known_args.table_id))


if __name__ == '__main__':
    logger = logging.getLogger()
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%m-%d %H:%M:%S')
    logging.root.setLevel(logging.INFO)
    run()
