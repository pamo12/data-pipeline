from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    sql = """
        INSERT INTO {}
        {}
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 target_table,
                 source_db,
                 select_sql,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.source_db = source_db
        self.select_sql = select_sql
        

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        formatted_sql = LoadDimensionOperator.sql.format(
            self.target_table,
            self.select_sql
        )
        self.log.info(formatted_sql)   
        redshift.run(formatted_sql)
