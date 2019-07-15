import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

class MyOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument(
        '--input_table',
        type=str,
        help='big query table to read from',
        default='broad-dsp-spec-ops.joint_genotyping_chr20_dalio_40000_updated')
    parser.add_value_provider_argument(
        '--output_bucket',
        type=str,
        help='bucket to output part files in',
        default='gs://lb_spec_ops_test/outputs/')
    parser.add_value_provider_argument(
        '--other_output_bucket',
        type=str,
        help="other output bucket that I'm not sure what it's for",
        default='gs://lb_spec_ops_test/outputs2/'
    )

# class CustomSort(beam.DoFn):
#     def process(self, element, *args, **kwargs):
#         range_key = element[0]
#         positions = list(element[1])
#         positions.sort()


def run(argv=None):
  from apache_beam.options.pipeline_options import PipelineOptions

  schema_string="""{
  "type": "record",
  "name": "__root__",
  "fields": [
    {
      "name": "position",
      "type": [
        "null",
        "long"
      ]
    },
    {
      "name": "values",
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "__s_0",
          "fields": [
            {
              "name": "sample",
              "type": [
                "null",
                "string"
              ]
            },
            {
              "name": "state",
              "type": [
                "null",
                "string"
              ]
            },
            {
              "name": "ref",
              "type": [
                "null",
                "string"
              ]
            },
            {
              "name": "alt",
              "type": [
                "null",
                "string"
              ]
            },
            {
              "name": "AS_RAW_MQ",
              "type": [
                "null",
                "string"
              ]
            },
            {
              "name": "AS_RAW_MQRankSum",
              "type": [
                "null",
                "string"
              ]
            },
            {
              "name": "AS_QUALapprox",
              "type": [
                "null",
                "string"
              ]
            },
            {
              "name": "AS_RAW_ReadPosRankSum",
              "type": [
                "null",
                "string"
              ]
            },
            {
              "name": "AS_SB_TABLE",
              "type": [
                "null",
                "string"
              ]
            },
            {
              "name": "AS_VarDP",
              "type": [
                "null",
                "string"
              ]
            },
            {
              "name": "call_GT",
              "type": [
                "null",
                "string"
              ]
            },
            {
              "name": "call_AD",
              "type": [
                "null",
                "string"
              ]
            },
            {
              "name": "call_DP",
              "type": [
                "null",
                "long"
              ]
            },
            {
              "name": "call_GQ",
              "type": [
                "null",
                "long"
              ]
            },
            {
              "name": "call_PGT",
              "type": [
                "null",
                "string"
              ]
            },
            {
              "name": "call_PID",
              "type": [
                "null",
                "string"
              ]
            },
            {
              "name": "call_PL",
              "type": [
                "null",
                "string"
              ]
            }
          ]
        }
      }
    }
  ]
}"""

  pipeline_options = PipelineOptions()
  pipeline_options = pipeline_options.view_as(MyOptions)

  p = beam.Pipeline(options=pipeline_options)

  table = pipeline_options.input_table
  table_data = (
      p
      | 'Query Data from BQ' >>
      beam.io.Read(beam.io.BigQuerySource(
          query="""SELECT *
FROM `{table}.pet_ir` AS pet
LEFT OUTER JOIN `{table}.vet_ir` AS vet
USING (position, sample)
WHERE ( position >= 10000000 AND position < 10100000 )""".format(table=table),
          use_standard_sql=True)
      )
  )

  def get_position_key_range(element):
    range_multiplier=20000
    position = element['position']
    key = position // range_multiplier
    key_val = (key, element)
    return key_val

  def position_sorter(key_val, output_dir):
    from apache_beam.io.gcp import gcsio
    import avro.schema
    from avro.datafile import DataFileWriter
    from avro.io import DatumWriter
    from itertools import groupby

    key = key_val[0]
    vals = list(key_val[1])
    vals = sorted(vals, key=lambda x: int(x['position']))

    out_file_path = output_dir.get() + "{:06d}.avro".format(key)
    out_file = gcsio.GcsIO().open(out_file_path, 'wb')

    schema = avro.schema.parse(schema_string)
    writer = DataFileWriter(out_file, DatumWriter(), schema)

    def clean_record(record):
      cleaned = {k:v for (k,v) in record.items() if v is not None}
      cleaned.pop('position', None)
      return cleaned

    #for key, group in groupby(things, lambda x: x[0]):
    for position, values in groupby(vals, lambda x: int(x['position'])):
      cleaned_values = [ clean_record(record) for record in values]
      writer.append({"position": position, "values" : cleaned_values})

    writer.close()

    return out_file_path

  row_data = (table_data
              | 'Generate Range Keys' >> beam.Map(lambda element: get_position_key_range(element))
              | 'Group By Range Keys' >> beam.GroupByKey()
              | 'Custom Local Sorter' >> beam.Map(lambda key_multi_val: position_sorter(key_multi_val, pipeline_options.output_bucket)))

  row_data | 'Printing data' >> beam.io.WriteToText(pipeline_options.other_output_bucket)

  result = p.run()
  result.wait_until_finish()


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  run()