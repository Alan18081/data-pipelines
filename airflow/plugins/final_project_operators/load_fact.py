from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id='',
                 load_sql='',
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.load_sql=load_sql

    def execute(self, context):
        self.log.info('[LoadFactOperator] task {} is started'.format(self.task_id))
        redshift_hook = PostgresHook(self.redshift_conn_id)
        redshift_hook.run(self.load_sql)
        self.log.info('[LoadFactOperator] task {} is completed'.format(self.task_id))



