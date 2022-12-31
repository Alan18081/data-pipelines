import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id = '',
                 read_sql='',
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.read_sql = read_sql


    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        records = redshift_hook.get_records(self.read_sql)
        for r in records:
            logging.info(r)
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. Query '{self.read_sql}' returned no results")       
        logging.info(f"Data quality on table: Query '{self.read_sql}' check passed with {records[0][0]} record")

        