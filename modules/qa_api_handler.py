from . import re
from . import dt
from . import os
from . import rq
from . import json
from . import time
from .snowflake import SnowflakeHandler


class ApiHandler:
    project_dir = os.path.join(os.environ['userprofile'], 'PycharmProjects', 'QAAPI')
    sql_dir = os.path.join(project_dir, 'sql_templates')
    csv_dir = os.path.join(project_dir, 'csv')
    json_dir = os.path.join(project_dir, 'json')
    error_dir = os.path.join(project_dir, 'errors')
    temp_dir = os.path.join(project_dir, 'temp')
    split_dir = os.path.join(project_dir, 'split')

    def __init__(self, console_output=False, schema=None, contact_source='api'):
        self.contact_json = None
        self.eval_json = None
        self.question_json = None
        self.console_output = console_output
        self.sn = SnowflakeHandler(console_output=self.console_output, schema=schema)
        self.object_creation_time = time.time()
        self.table_pairs = {
            'forms.json': 'temp_calabrio_t_qa_forms_staging',
            'contacts.json': 'temp_calabrio_t_qa_contacts_staging',
            'fix_eval_raw.json': 'temp_calabrio_t_qa_evaluations_staging',
            'fix_comments_raw.json': 'temp_calabrio_t_qa_evaluation_comments_staging',
        }
        self.file_fixes = []
        self.file_fix_re = re.compile(r']\n\[')
        self.file_uploads = []
        self.contact_source = contact_source
        self._login()

    def _login(self):
        # login to session
        if self.console_output:
            print(f'Starting requests session')
        self.session = rq.Session()
        if self.console_output:
            print(f'Session verification: {self.session.verify}')
        auth_url = "https://calabriocloud.com/api/rest/authorize"
        auth_payload = os.environ['CALABRIO_KEY']
        auth_req = rq.Request("POST", auth_url, data=auth_payload)
        auth_prepped = self.session.prepare_request(auth_req)
        auth_resp = self.session.send(auth_prepped)
        if self.console_output:
            print(f'authorization response code: {auth_resp.status_code}')

    def _get_forms(self):
        base_url = 'https://calabriocloud.com/api/rest/recording/evalform'
        begin = time.time()
        self.contact_json = json.loads(self.session.get(base_url).text)
        end = time.time()
        if self.console_output:
            print(
                f'''self.contact_json was populated in {round(end - begin, 4)} seconds.\n
                    The program has ran for {round(time.time() - self.object_creation_time, 4)} seconds.''')
        with open(os.path.join(self.json_dir, 'forms.json'), 'w') as cf:
            cf.write(json.dumps(self.contact_json))

    def _get_contacts(self):
        if self.contact_source == 'api':
            base_url = 'https://calabriocloud.com/api/rest/recording/contact'
            begin_date = dt.date.today() + dt.timedelta(days=-30)
            # begin_date = dt.date.today() + dt.timedelta(days=-57)  # Temp date delta to back-fill holes.
            # params = {'beginDate': '2019-03-01',  # free-text start date
            params = {'beginDate': begin_date.strftime('%Y-%m-%d'),
                      'evalState': 'scored',
                      'limit': 50000}
            query_url = base_url + '?' + '&'.join([f'{x}={params[x]}' for x in params])
            begin = time.time()
            self.contact_json = json.loads(self.session.get(query_url).text)
            end = time.time()
            if self.console_output:
                print(f'''
    self.contact_json was populated in {round(end - begin, 4)} seconds.\n
    The program has ran for {round(time.time() - self.object_creation_time, 4)} seconds.
''')
            with open(os.path.join(self.json_dir, 'contacts.json'), 'w') as cf:
                cf.write(json.dumps(self.contact_json))
        elif self.contact_source == 'sql':
            # print('do sql source')
            self.sn.set_con_and_cur()
            self.contact_json = self.sn.run_query_file(os.path.join(self.sql_dir, 'trouble_children.sql'))
            self.sn.close_con_and_cur()
        else:
            return 'oops, wrong source...'

    def _get_evaluations(self):
        eval_url_raw = 'https://calabriocloud.com/api/rest/recording/contact/<<contact_id>>/eval/'
        rn = len(self.contact_json)
        for row in self.contact_json:
            print(f'debugging: len(row) from self.contact_json\n{len(row)}')
            eval_replacement = ''
            if self.contact_source == 'api':
                eval_replacement = str(row['id'])
            elif self.contact_source == 'sql':
                eval_replacement = str(row[0])
            eval_url_query = eval_url_raw.replace('<<contact_id>>', eval_replacement)
            print(f'debugging: eval_url_query\n{eval_url_query}')
            begin = time.time()
            eval_res = self.session.get(eval_url_query)
            with open(os.path.join(self.temp_dir, 'eval_raw.json'), 'ab') as ab:
                ab.write(eval_res.content)
            eval_json_response = eval_res.json()
            end = time.time()
            if self.console_output:
                print(f'''    Writing to eval_raw.json and making the eval_json_response variable had a duration of
    {round(end - begin, 4)} seconds.
    The program has ran for {round(time.time() - self.object_creation_time, 4)} seconds.
    rn: {rn}
    Progress: {round(((len(self.contact_json) - rn) / len(self.contact_json)) * 100, 2)}%'''
                      )
            if self.eval_json is None:
                self.eval_json = eval_json_response
            else:
                for jr in eval_json_response:
                    # print(f'debug: jr in eval_json_response:\nval|{jr}\nlen|{len(jr)}')
                    self.eval_json.append(jr)
            rn -= 1
            print(f'self.eval_json len == {len(self.eval_json)}')
        self._fix_file(os.path.join(self.temp_dir, 'eval_raw.json'))

    def _get_comments(self):
        # Last rn attempted:
        print('tbd')
        comments_url_base = 'https://calabriocloud.com'
        print(f"""
    len(eval_json): {len(self.eval_json)}
""")
        rn = len(self.eval_json)
        with open(os.path.join(self.temp_dir, 'comments_raw.json'), 'ab') as wf:
            for jr in self.eval_json:
                if 'comments' in jr:
                    print(f'jr["comments"] debug: {jr["comments"]}')
                    comments_url_query = comments_url_base + jr["comments"]
                    print(f'comments_url_query: {comments_url_query}')
                    begin = time.time()
                    comments_res = self.session.get(comments_url_query)
                    wf.write(comments_res.content)
                    end = time.time()
                    print(f'''    Writing to eval_raw.json had a duration of {round(end - begin, 4)} seconds.
    The program has ran for {round(time.time() - self.object_creation_time, 4)} seconds.
    rn: {rn}
    Progress: {round(((len(self.eval_json) - rn) / len(self.eval_json)) * 100, 2)}%'''
                          )
                else:
                    print(f'jr: {jr["id"]} did not have any comments.')
                rn -= 1
        self._fix_file(os.path.join(self.temp_dir, 'comments_raw.json'))

    def _fix_file(self, file):
        file_name = os.path.basename(file)
        with open(file, 'r') as rf:
            file_text = rf.read()
            new_text = self.file_fix_re.sub(',\n', file_text)
            with open(os.path.join(self.json_dir, 'fix_' + file_name), 'w') as nf:
                nf.write(new_text)

    def _split_files(self):
        """
        Only use when needed.
        :return: None
        :rtype: None
        """
        # todo: Setup the split
        # todo: Setup the daily total contacts pull
        # files_to_split = os.listdir(self.json_dir)
        # for file in files_to_split:
        #     with open(os.path.join(self.json_dir, file), 'r') as rf:

    def full_run(self):
        self._remove_temp_files()
        fun_list = [
            # self._get_forms,
            self._get_contacts,
            self._get_evaluations,
            # self._get_comments,
        ]
        for fun in fun_list:
            print(f'running function: {fun.__name__}')
            fun()
            print(f'function: {fun.__name__} ran with no errors.\n')
        json_files = os.listdir(self.json_dir)
        for file in json_files:
            # with open(os.path.join(self.json_dir, file)) as f:
            #     first_result = json.loads(f.read())
            #     print(f'displaying file: {file}')
            #     self._print_results(first_result)
            with open(os.path.join(self.sql_dir, 'truncate_table.sql')) as tf:
                self.sn.sql_command(tf.read().replace('<<tn>>', self.table_pairs[file]))
            with open(os.path.join(self.sql_dir, 'stage_file.sql'), 'r') as sf:
                query_text_raw = sf.read()
                query_text_raw = query_text_raw.replace('<<fp>>', os.path.join(self.json_dir, file))
                query_text_final = query_text_raw.replace('<<ac>>', 'true')
                # Eventually turn the true/false value into a user defined dynamic field.
                print(query_text_final)
                self.sn.sql_command(query_text_final)
            with open(os.path.join(self.sql_dir, 'populate_table.sql')) as cf:
                copy_text_raw = cf.read()
                copy_text_raw = copy_text_raw.replace('<<tn>>', self.table_pairs[file])
                copy_text_final = copy_text_raw.replace('<<pat>>', file)
                print(copy_text_final)
                self.sn.sql_command(copy_text_final)
        self.sn.sql_command('remove @my_uploader_stage')
        self._remove_temp_files()
        print(f'The {__name__} function ended after {round(time.time() - self.object_creation_time, 4)} seconds.')

    def _remove_temp_files(self):
        tmp_files = os.listdir(self.temp_dir)
        print(f'file list:\n{tmp_files}')
        for file in tmp_files:
            os.remove(os.path.join(self.temp_dir, file))
        print(f'tmp_dir after os.remove:\n{os.listdir(self.temp_dir)}')

    @staticmethod
    def _print_results(json_var):
        for row in json_var[:3]:
            for k in row:
                print(f'\t{k} | {row[k]}')
            print('\n')
