from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    sql_statement = """
        COPY {table}
        FROM 's3://{s3_bucket_name}/{s3_prefix}/'
        CREDENTIALS 'aws_access_key_id={aws_access_key};aws_secret_access_key={aws_secret_key}'
        region 'us-east-1' FORMAT AS JSON '{s3_json_path}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_conn_id='',
                 table='',
                 s3_bucket_name='',
                 s3_prefix='',
                 s3_json_path='auto',
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_conn_id = aws_credentials_conn_id
        self.table = table
        self.s3_bucket_name = s3_bucket_name
        self.s3_prefix = s3_prefix
        self.s3_json_path = s3_json_path

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')
        metastoreBackend = MetastoreBackend()
        aws_connection = metastoreBackend.get_connection(self.aws_credentials_conn_id)
        redshift_hook = PostgresHook(self.redshift_conn_id)
        redshift_hook.run(self.sql_statement.format(
            table=self.table,
            s3_bucket_name=self.s3_bucket_name,
            s3_prefix=self.s3_prefix,
            aws_access_key=aws_connection.login,
            aws_secret_key=aws_connection.password,
            s3_json_path=self.s3_json_path
        ))
        self.log.info('StageToRedshiftOperator body is executed successfully')






