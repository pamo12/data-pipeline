from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 tables,
                 redshift_conn_id,
                 sql,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.sql = sql
        

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        
        tables = [x.strip() for x in self.tables.split(',')]
        checks_passed = True
        affected_tables = []
        
        # checks quality for each given table 
        for table in tables:
            formatted_sql = self.sql.format(table)
            self.log.info(f'Data quality check executed will be executed for Table {table}')
            self.log.info(f'SQL to be executed: {formatted_sql}')
            records = redshift.get_records(formatted_sql)
            num_records = records[0][0]
            if len(records) < 1 or len(records[0]) < 1:
                checks_passed = False
                affected_tables.append(table)

        if not checks_passed:
            raise ValueError(f'Data quality check failed for Tables {affected_tables}')
               
        self.log.info(f'Data quality checks have been successful for {self.tables}')