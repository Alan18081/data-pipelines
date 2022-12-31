from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTableOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id='',
                 table_name='',
                 create_sql='',
                 *args, **kwargs):

        super(CreateTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.create_sql = create_sql

    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        redshift_hook.run("drop table if exists {}".format(self.table_name))
        redshift_hook.run(self.create_sql)
        
        redshift_hook.get_records("select * from {} limit 1".format(self.table_name))


        

