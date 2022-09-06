import os
import argparse
#import psycopg2
import pg8000.dbapi
import logging
import pytest
import json
from typing import Generator
#importrpytestL, 105B written
import apache_beam as beam
import flask
from flask import jsonify
from flask_sqlalchemy import sqlalchemy
from google.cloud.sql.connector import Connector
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options_validator import PipelineOptionsValidator
from apache_beam.utils.annotations import deprecated

#**** To establish a Postgres connection and executing required queries

class db_data(beam.DoFn):
   # def process(self,element):
    def __init__(self,data_req):
       self.data_req=data_req
    def init_connection_engine(self) -> sqlalchemy.engine.Engine:
     def getconn() -> pg8000.dbapi.Connection:
        # initialize Connector object for connections to Cloud SQL
        with Connector() as connector:
            conn: pg8000.dbapi.Connection = connector.connect(
                     "sonam-sandbox-356306:australia-southeast1:sub-act",
                 "pg8000",
                 user="postgres",
                 password="sub-act-df",
                 db="subscriber-act",
             )
            return conn
    # create SQLAlchemy connection pool
     pool = sqlalchemy.create_engine(
             "postgresql+pg8000://",
              creator=getconn,
            )
     pool.dialect.description_encoding = None
     print("to test connection")
     return pool
      # [END cloud_sql_connector_postgres_pg8000]
#@pytest.fixture(name="pool")
  #def setup() -> Generator:
 #def app_context():
  #  with app.app_context():
    def read_data_req(self):
      if self.data_req == 'voice_call':
          return(sqlalchemy.text("SELECT subscription_id,actual_duration FROM voice_call_record;"))
      if self.data_req == 'sms_rec':
          return(sqlalchemy.text("SELECT subscription_id,network_id FROM sms_record;"))
      elif self.data_req == 'data_volume':
          return(sqlalchemy.text("SELECT subscription_id,uplink_volume,downlink_volume FROM mobile_data_activity;"))
     # return (select_stmnt)    
    def process(self,element):
     pool = self.init_connection_engine()
    #def test_pooled_connection_with_pg8000(pool: sqlalchemy.engine.Engine) -> None:
   #  select_stmt = sqlalchemy.text("SELECT subscription_id,uplink_volume,downlink_volume FROM mobile_data_activity;")
     select_stmt= self.read_data_req()
     print(select_stmt)
     with pool.connect() as conn:
        rows= conn.execute(select_stmt).fetchall()
       # i =0
        for row in rows:
                    
          yield (dict(row))
          
          
          
def run():
   table_spec='sonam-sandbox-356306:mob_net.subscriber_activity_snapshotfinal'
#table_schema='subscription_id:INTEGER,shortest_call_made:INTEGER,longest_call_made:INTEGER,total_number_sms:INTEGER,total_data_uploaded:INTEGER,total_data_downloaded:INTEGER'
   table_schema={
    "fields": [
    {
        "name": "subscription_id", "type": "INTEGER", "mode": "NULLABLE",
    },
    {
        "name": "shortest_call", "type": "INTEGER", "mode": "NULLABLE"
    },
    {
        "name": "longest_call", "type": "INTEGER", "mode": "NULLABLE"
    },
    {
        "name": "total_sms_sent", "type": "INTEGER", "mode": "NULLABLE"
    },
    {
        "name": "total_uplink_vol", "type": "INTEGER", "mode": "NULLABLE"
    },
     {
        "name": "total_downlink_vol", "type": "INTEGER", "mode": "NULLABLE"
    }]
}
   parser = argparse.ArgumentParser()
# parser.add_argument('--my-arg', help='description')
   args, beam_args = parser.parse_known_args()
   beam_options = PipelineOptions(
    beam_args,
    runner='DataflowRunner',
    project='sonam-sandbox-356306',
    job_name='subscr-activity-job',
    temp_location='gs://ss_df_exercise/temp',
    region='australia-southeast2'

                                 )
   with beam.Pipeline(options=beam_options) as pipeline:
     

#******* calculating sms records per subscrption id from sms_record ****
       trans_sms_data=(
            pipeline
            |'Pipeline start for sms data'>>beam.Create([])
            |'Sms Record'>>beam.ParDo(db_data(data_req='sms_rec'))
            |'Pair the values'>> beam.Map(lambda x: (x['subscription_id'],x['network_id']))
           # | "To string" >> beam.ToString.Iterables()
            | "Count of SMS" >> beam.combiners.Count.PerKey()
                      )
            
#***** calculating shortest and longest voice call duration from voic_call_record ****        
       trans_voice_data=(
            pipeline
            |'Pipeline start for voice data'>>beam.Create([{}])
            |'Voice Record'>>beam.ParDo(db_data(data_req='voice_call'))
           # |'voice data'>>beam.Map(print) 
            |'Pairing'>>beam.Map(lambda x:(x['subscription_id'],x['actual_duration']))
       #     |beam.Map(print)
            )



if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
 # process()
