from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    template_fields = ("s3_key",)
   
    load_dimension_table_insert = """
    INSERT INTO {} {}
    """
    load_dimension_table_truncate = """
    TRUNCATE TABLE {} 
    """
    @apply_defaults
    def __init__(self,
             query="",
             redshift_conn_id="",
             t_name="",
             operation="",
             *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.query=query
        self.redshift_conn_id=redshift_conn_id
        self.t_name=t_name
        self.operation=operation
    
    def execute(self, context):
        self.log.info(f"Started LoadDimensionOperator {self.t_name} started with mode {self.operation} ")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if(self.operation == "append"):
            redshift_hook.run(LoadDimensionOperator.load_dimension_table_insert.format(self.t_name, self.query)) 
        if(self.operation == "truncate"):
            redshift_hook.run(LoadDimensionOperator.load_dimension_table_truncate.format(self.t_name)) 
            redshift_hook.run(LoadDimensionOperator.load_dimension_table_insert.format(self.t_name, self.query)) 
        self.log.info(f"Ending LoadDimensionOperator {self.t_name} with a Success on Operation  {self.operation}")

    