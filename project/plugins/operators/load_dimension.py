from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    sql = """
        INSERT INTO {}
        {}
    """
    
    truncate_sql = """
        TRUNCATE TABLE {};
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 target_table,
                 source_db,
                 select_sql,
                 refresh_table,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.source_db = source_db
        self.select_sql = select_sql
        self.refresh_table = refresh_table
        

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.refresh_table:
            self.log.info(f'Going to truncate Table {self.target_table}') 
            formatted_truncate_sql = LoadDimensionOperator.truncate_sql.format(
                self.target_table
            )
            redshift.run(formatted_truncate_sql)
        
        formatted_sql = LoadDimensionOperator.sql.format(
            self.target_table,
            self.select_sql
        )
        self.log.info(f'Going to execute {formatted_sql}')   
        redshift.run(formatted_sql)
