"""Performs data quality checks on Redshift data"""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 target_schema="",
                 map_table_column=[],
                 check_input =[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_schema = target_schema
        self.map_table_column=map_table_column
        self.check_input = check_input

    def execute(self, context):
        
        self.log.info("Getting credentials")
        redshift_hook = PostgresHook(self.redshift_conn_id) 

        for tab, col in self.map_table_column.items(): # iterate over table and column pairs defined in given map
            self.log.info(f"check for target table: {tab}")
            for check_in in self.check_input:
                query_template = check_in.get("check_query")
                query = query_template.format(schema=self.target_schema, table=tab, column=col)
                expected_result = check_in.get("expected_result")
                check_condition = check_in.get("check_condition")
                check_feature = check_in.get("check_feature")
                self.log.info(f"Feature to be checked: {check_feature}")
                self.log.info(f"Query to be executed: {query}")
                self.log.info(f"Expected result: {expected_result}")
                self.log.info(f"Check condition: {check_condition}")

                records = redshift_hook.get_records(query)
                check_result = records[0][0]
                self.log.info(f"Check result for feature {check_feature}:{check_result}")

                # evaluate results
                check_pass = False # default value for verdict

                if check_condition == 'equal':
                    if check_result == expected_result:
                        check_pass = True

                elif check_condition == 'greater than':
                    if check_result > expected_result:
                        check_pass = True

                elif check_condition == 'less than':
                    if check_result < expected_result:
                        check_pass = True 
                else:
                    raise ValueError("Unrecognized check condition!")

                if check_pass:
                    self.log.info(f"Passed quality check for {tab}")
                else:
                    raise ValueError(f"Failed quality check for {tab} for feature {check_feature}.")
                          
        self.log.info(f"Success: {self.task_id}")