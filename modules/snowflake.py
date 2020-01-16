from . import os
from . import json
import snowflake.connector


class SnowflakeHandler:
    """
    Prerequisites:
        1) You need to create a JSON formatted Environment Variable, named "SNOWFLAKE_KEY", with the following keys.
            1) USERNAME
            2) PASSWORD
            3) ACCOUNT
            4) WAREHOUSE
            5) DATABASE
        Please see Snowflake documentation for the definitions of the required fields.
    """

    dl_dir = os.path.join(os.environ['userprofile'], 'Downloads')

    def __init__(self, console_output=False, schema=None):
        self.console_output = console_output
        self.user = ''
        self.password = ''
        self.account = ''
        self.warehouse = ''
        self.database = ''
        self.temp_query = ''
        self.schema = schema
        self.con = False
        self.cur = False
        self._set_credentials()

    def _set_credentials(self):
        if self.console_output:
            print('Collecting Snowflake credentials from system environment...')
        snowflake_json = json.loads(os.environ['SNOWFLAKE_KEY'])
        self.user = snowflake_json['USERNAME']
        self.password = snowflake_json['PASSWORD']
        self.account = snowflake_json['ACCOUNT']
        self.warehouse = snowflake_json['WAREHOUSE']
        self.database = snowflake_json['DATABASE']
        if self.console_output:
            print('credentials have been collected and assigned')

    def set_con_and_cur(self):
        self.con = snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema
        )
        self.cur = self.con.cursor()

    def close_con_and_cur(self):
        self.cur.close()
        self.con.close()
        self.cur = False
        self.con = False

    def run_query_file(self, file_path):
        """
        :param file_path: the path to the sql file that needs to be executed
        :return: A list of tuples containing the query result data.
        """
        if self.cur:
            with open(file_path, 'r') as f:
                query_string = f.read()
            results = self.cur.execute(query_string)
            data = results.fetchall()
            return data
        else:
            return 'self.cur evaluated to False'

    def run_query_string(self, query_string):
        """
        :param query_string: a string of sql
        :return: A list of tuples containing the query result data.
        """
        if self.cur:
            results = self.cur.execute(query_string)
            data = results.fetchall()
            return data
        else:
            return 'self.cur evaluated to False'

    def sql_command(self, command_string):
        self.set_con_and_cur()
        if self.cur:
            response = self.cur.execute(command_string)
            self.con.commit()
        else:
            return 'self.cur evaluated to False'
        self.close_con_and_cur()

    def stage_file_data(self, full_file_path):
        if self.cur:
            self.cur.execute(f'put file://{full_file_path} @MY_UPLOADER_STAGE auto_compress=TRUE')
        else:
            print('connection and cursor are not established. The data cannot be staged.')
