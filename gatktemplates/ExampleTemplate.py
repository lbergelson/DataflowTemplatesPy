import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions


class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--input_csv_file',
            type=str,
            help='path of input csv file',
            default='gs://my-bucket/input')
        parser.add_value_provider_argument(
            '--output_fhir_json',
            type=str,
            help='path of output fhir json files',
            default='gs://my-bucket/output')


class generateFHIR(beam.DoFn):
    """Adding key rows to process"""

    def process(self, element, *args, **kwargs):
        # Some Logic!
        yield ""


def run():
    pipeline_options = PipelineOptions().view_as(MyOptions)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.region = 'us-east1'
    google_cloud_options.project = 'PRJ-ID'
    google_cloud_options.job_name = 'csv-fhir-json'
    google_cloud_options.staging_location = 'gs://tewariy-dataflow-jobs/staging'
    google_cloud_options.temp_location = 'gs://tewariy-dataflow-jobs/temp'
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'
    # pipeline_options.view_as(StandardOptions).runner = 'DirectRunner'

    input_csv_file = str(pipeline_options.input_csv_file)
    input_csv_file = input_csv_file.strip()
    # input_csv_file = 'gs://msk-poc-csv/csv/Mock_Lab_Results.csv'
    output_fhir_json = str(pipeline_options.output_fhir_json)
    output_fhir_json = output_fhir_json.strip()

    logging.info("Pipleline options QQQQQQQQQ :" + input_csv_file)
    logging.info("Pipleline options QQQQQQQQQ :" + output_fhir_json)

    p = beam.Pipeline(options=pipeline_options)

    data = p | "Reading from GS csv file" >> beam.io.ReadFromText(input_csv_file,
                                                                  skip_header_lines=1)
    dicts = data | "Add keys for columns" >> beam.ParDo(
        generateFHIR()) | "Individual Write to GCS" >> beam.io.WriteToText(output_fhir_json)

    dicts | "Write to GCS" >> beam.io.WriteToText(output_fhir_json)

    result = p.run()
    result.wait_until_finish()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()

