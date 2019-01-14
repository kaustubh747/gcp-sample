import apache_beam as beam
import sys

from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import logging

PROJECT='bd-df-poc'
BUCKET='bq-df-poc-bucket'
schema = 'id:INTEGER,fname:STRING,lname:STRING,email:STRING,gender:STRING,ipaddr:STRING'

class Split(beam.DoFn):
    def process(self, element):
        id, fname, lname, email, gender, ipaddr = element.split(",")
        return [{
            'id': int(id),
            'fname': fname,
            'lname': lname,
            'email':email,
            'gender':gender,
            'ipaddr':ipaddr
        }]
        
def run(argv=None):

    options = GoogleCloudOptions()
    options.view_as(StandardOptions).runner = 'DataflowRunner'
    options.project = 'bd-df-poc'
    options.job_name = 'gcs2bq'
    options.staging_location = 'gs://bq-df-poc-bucket/staging'
    options.temp_location = 'gs://bq-df-poc-bucket/temp'

    pipeline = beam.Pipeline(options=options)

    (pipeline
      | 'ReadCSV' >> beam.io.ReadFromText('gs://{0}/sample_emp_data.csv'.format(BUCKET))
      | 'Convert' >> beam.ParDo(Split())
      | 'writeToBQ' >> beam.io.WriteToBigQuery('{0}:sample.employee'.format(PROJECT),
                                               schema=schema,
                                               write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                               create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    )
    
    pipeline.run().wait_until_finish()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
