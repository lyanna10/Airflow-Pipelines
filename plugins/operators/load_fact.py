from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    load_fact_table_insert = """
    INSERT INTO {} {}
    """
    load_fact_table_truncate = """
    TRUNCATE TABLE {} 
    """
    @apply_defaults
    def __init__(self,
             query="",
             redshift_conn_id="",
             t_name="",
             operation="",
             *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.query=query
        self.redshift_conn_id=redshift_conn_id
        self.t_name=t_name
        self.operation=operation
    
    def execute(self, context):
        self.log.info(f"Started LoadFactOperator {self.t_name} started with mode {self.operation} ")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if(self.operation == "append"):
            redshift_hook.run(LoadFactOperator.load_fact_table_insert.format(self.t_name, self.query)) 
        self.log.info(f"Ending LoadFactOperator {self.t_name} with a Success on Operation  {self.operation}")
