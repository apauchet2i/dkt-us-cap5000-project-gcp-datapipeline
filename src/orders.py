import argparse
import datetime
from sys import argv
import apache_beam as beam
from apache_beam.examples.cookbook.coders import JsonCoder
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions


def parseShopifyJson(dict_input):
    dict_bigquery = {}
    dict_bigquery["number"] = dict_input["name"]
    dict_bigquery["customer_id"] = dict_input["customer"]["id"]
    dict_bigquery["street1"] = dict_input["shipping_address"]["address1"]
    dict_bigquery["street2"] = dict_input["shipping_address"]["address2"]
    dict_bigquery["zip_code"] = dict_input["shipping_address"]["zip"]
    dict_bigquery["city"] = dict_input["shipping_address"]["city"]
    dict_bigquery["country"] = dict_input["shipping_address"]["country"]
    dict_bigquery["created_at"] = dict_input["created_at"][:-6]
    dict_bigquery["updated_at"] = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    return dict_bigquery


def shopifyOrdersPipeline():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--input',
        dest='input',
        required=False,
        help='Input file to read',
        default='gs://dkt-us-ldp-baptiste-test/webhookShopify-05_01_2021_10_11_36.json')

    parser.add_argument('--output_table',
                        dest='output',
                        required=False,
                        help='Output BQ table to write results to.',
                        default='dkt-us-data-lake-a1xq:dkt_us_test_cap5000.orders')

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_args.extend([
        '--runner=DirectRunner',
        '--project=dkt-us-data-lake-a1xq',
        '--staging_location=gs://dkt-us-ldp-baptiste-test/staging',
        '--temp_location=gs://dkt-us-ldp-baptiste-test/temp',
        '--job_name=shopifyjob',
        '--region=us-central1',
        '--subnetwork=https://www.googleapis.com/compute/v1/projects/dkt-us-data-lake-a1xq/regions/us-central1/subnetworks/data-fusion-network'
    ])

    p = beam.Pipeline(options=PipelineOptions(pipeline_args))

    (
            p | 'Read data' >> ReadFromText(known_args.input, coder=JsonCoder())
            | 'String To BigQuery Row' >> beam.Map(lambda s: parseShopifyJson(s))
            | 'Write to Table' >> beam.io.WriteToBigQuery(

        known_args.output,
        schema='number:STRING,customer_id:STRING,street1:STRING,street2:STRING,zip_code:STRING,city:STRING,country:STRING,created_at:DATETIME, updated_at:DATETIME',
        method="STREAMING_INSERTS",
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))

    p.run()
#comment