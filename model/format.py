import apache_beam as beam


class CreateEntities(beam.DoFn):
    def __init__(self, project, kind, model=None):
        if model is None:
            self.kind = kind
        else:
            self.kind = kind + '-' + model
        self.project = project

    def process(self, element, timestamp=None, *args, **kwargs):
        from apache_beam.io.gcp.datastore.v1new import types

        kind_id = timestamp.strftime('%Y%m%d-%H%M%S') + '-' + str(element.pop('indice'))
        key = types.Key([self.kind, kind_id], project=self.project)
        entity = types.Entity(key=key)
        entity.set_properties({
            'user_id': element['user_id'],
            'item_id': element['item_id'],
            'created': timestamp,
            'rule': 'RULE_NAME',
        })
        yield entity


class CreateDirectRow(beam.DoFn):
    def process(self, element, timestamp, *args, **kwargs):
        from google.cloud.bigtable import row

        pids = ','.join(element['item_id'])
        row_key = element['user_id']

        direct_row = row.DirectRow(row_key)
        direct_row.set_cell('umaylike', 'user_id', pids, timestamp)
        direct_row.set_cell('umaylike', 'rule', 'RULE_NAME', timestamp)
        yield direct_row
    