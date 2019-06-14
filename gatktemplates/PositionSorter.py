import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.io.gcp import gcsio


def position_key_ranges(range_multiplier=200000, num_partitions=2000):
    for positionKeyRange in [((steps - 1) * range_multiplier) for steps in range(num_partitions, 0, -1)]:
        yield positionKeyRange


def get_position_key_range(element):
    position = element['start_position']

    for i in position_key_ranges():
        if position >= i:
            print (str(position) + " has the range key of : " + str(i))
            key_val = (i, position)
            return key_val
    print (str(position) + " has the range key of : " + str(1))
    key_val = (0, position)
    return key_val


def position_sorter(key_val):
    key = key_val[0]
    vals = list(key_val[1])
    vals.sort()

    out_file_path = 'gs://tewariy-dataflow-jobs/outputs/range-outputs-2/' + str(key) + '.csv'
    out_file = gcsio.GcsIO().open(out_file_path, 'w')
    for pos in vals:
        row = str(key) + ',' + str(pos)
        out_file.write(row)
        out_file.write('\n')
    out_file.close()


# class CustomSort(beam.DoFn):
#     def process(self, element, *args, **kwargs):
#         range_key = element[0]
#         positions = list(element[1])
#         positions.sort()


def run(argv=None):
    pipeline_options = PipelineOptions()
    p = beam.Pipeline(options=pipeline_options)

    table_data = (
            p
            | 'Query Data from BQ' >>
            beam.io.Read(beam.io.BigQuerySource(
                query='select * from `tewariy-20181112.cross_join_tests.start_position_clustered`',
                use_standard_sql=True)
            )
    )

    row_data = (table_data
                | 'Generate Range Keys' >> beam.Map(lambda element: get_position_key_range(element))
                | 'Group By Range Keys' >> beam.GroupByKey()
                | 'Custom Local Sorter' >> beam.Map(lambda key_multi_val: position_sorter(key_multi_val)))

    row_data | 'Printing data' >> beam.io.WriteToText('gs://tewariy-dataflow-jobs/outputs/table-data-3')

    result = p.run()
    # result.wait_until_finish()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()