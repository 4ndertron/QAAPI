from . import re
from . import dt
from . import os
from . import rq
from . import json
from . import time
from . import sql  # todo: implement the sqlite database at a later time for reporting running performances.
from .snowflake import SnowflakeHandler


class ApiHandler:
    project_dir = os.path.join(os.environ['userprofile'], 'PycharmProjects', 'QAAPI')
    sql_dir = os.path.join(project_dir, 'sql_templates')
    csv_dir = os.path.join(project_dir, 'csv')
    json_dir = os.path.join(project_dir, 'json')
    error_dir = os.path.join(project_dir, 'errors')
    temp_dir = os.path.join(project_dir, 'temp')
    split_dir = os.path.join(project_dir, 'split')
    db_dir = os.path.join(project_dir, 'local_db')

    def __init__(self
                 , console_output=False
                 , schema=None
                 , all_contacts=False
                 , begin_date=''
                 , end_date=''
                 , break_size=None
                 , sql_file=None
                 ):
        self.forms_json = None
        self.contact_json = None
        self.eval_json = None
        self.question_json = None
        self.all_contacts = all_contacts
        self.begin_date = dt.date.fromisoformat(begin_date)
        self.end_date = dt.date.fromisoformat(end_date)
        self.break_size = dt.timedelta(int(break_size))
        self.transcript_json = []
        self.console_output = console_output
        self.sn = SnowflakeHandler(console_output=self.console_output, schema=schema)
        self.object_creation_time = time.time()
        self.all_contacts_re = re.compile('all_contacts')
        self.table_pairs = {
            'forms.json': 'temp_calabrio_t_qa_forms_staging',
            'contacts_1.json': 'temp_calabrio_t_qa_contacts_staging',
            'all_contacts*.json': 'temp_calabrio_t_contacts_staging',
            'fix_eval_raw.json': 'temp_calabrio_t_qa_evaluations_staging',
            'fix_comments_raw.json': 'temp_calabrio_t_qa_evaluation_comments_staging',
            'fix_transcript_raw.json': 'temp_calabrio_t_qa_transcripts_staging',
        }
        self.file_fixes = []
        self.file_fix_re = re.compile(r']\n\[')
        self.file_uploads = []
        self.query_file = sql_file
        self.file_type = '.json'
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

    def get_forms(self):
        file_name = 'forms' + self.file_type
        base_url = 'https://calabriocloud.com/api/rest/recording/evalform'
        begin = time.time()
        form_res = self.session.get(base_url)
        self.forms_json = form_res.json()
        with open(os.path.join(self.json_dir, file_name), 'wb') as cf:
            cf.write(form_res.content)
        end = time.time()
        if self.console_output:
            print(f'''    get_forms was populated in {round(end - begin, 4)} seconds.\n
    The program has ran for {round(time.time() - self.object_creation_time, 4)} seconds.''')

    def get_all_contacts(self):
        batch = 1
        if self.query_file is None:  # if a query file was not passed into the object creation, then run the api.
            base_url = 'https://calabriocloud.com/api/rest/recording/contact'
            days = self.end_date - self.begin_date
            for i in range(0, days.days, self.break_size.days):
                if self.all_contacts:
                    file_name = f'all_contacts_{str(batch)}' + self.file_type
                else:
                    file_name = f'contacts_{str(batch)}' + self.file_type
                local_start = self.begin_date + dt.timedelta(i)
                if self.begin_date + dt.timedelta(i + self.break_size.days) < self.end_date:
                    local_end = self.begin_date + dt.timedelta(i + self.break_size.days)
                else:
                    local_end = self.end_date
                params = {'beginDate': local_start.strftime('%Y-%m-%d'),
                          'endDate': local_end.strftime('%Y-%m-%d'),
                          'limit': 50000}
                if self.all_contacts:
                    pass
                else:
                    params['evalState'] = 'scored'
                query_url = base_url + '?' + '&'.join([f'{x}={params[x]}' for x in params])
                print(f'debugging query_url for get_all_contacts function in batch: {batch}\n{query_url}')
                begin = time.time()
                self.contact_json = json.loads(self.session.get(query_url).text)
                end = time.time()
                if self.console_output:
                    print(f'''    self.contact_json was populated in {round(end - begin, 4)} seconds.\n
    The program has ran for {round(time.time() - self.object_creation_time, 4)} seconds.
''')
                with open(os.path.join(self.json_dir, file_name), 'w') as cf:
                    cf.write(json.dumps(self.contact_json))
                batch += 1
        elif self.query_file is not None:  # if a query file was passed into the object creation, then run the query.
            self.sn.set_con_and_cur()
            self.contact_json = self.sn.run_query_file(os.path.join(self.sql_dir, self.query_file))
            self.sn.close_con_and_cur()
        else:
            return 'oops, wrong source...'

    def get_evaluations(self):
        file_name = 'evaluations' + self.file_type
        eval_url_raw = 'https://calabriocloud.com/api/rest/recording/contact/<<contact_id>>/eval/'
        rn = len(self.contact_json)
        for row in self.contact_json:
            print(f'debugging: len(row) from self.contact_json\n{len(row)}')
            eval_replacement = ''
            if self.query_file is None:
                eval_replacement = str(row['id'])
            elif self.query_file is not None:
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
    {__name__} Progress: {round(((len(self.contact_json) - rn) / len(self.contact_json)) * 100, 2)}%'''
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

    def get_transcripts(self):
        """
        This function will collect all the transcripts for a given collection of Calabrio Contact ID's.
        :return:
        :rtype:
        """
        transcript_url_base = 'https://calabriocloud.com/api/rest/cas/speechtextview'
        rn = len(self.contact_json)
        file_name = 'transcripts' + self.file_type
        for row in self.contact_json:
            transcript_replacement = ''
            if self.query_file is None:
                transcript_replacement = str(row['id'])
            elif self.query_file is not None:
                transcript_replacement = str(row[0])
            params = {
                'ccrid': transcript_replacement,
                'isRootRecording': 'false'
            }
            query_url = transcript_url_base + '?' + '&'.join([f'{x}={params[x]}' for x in params])
            print(f'debugging: transcript_url_query\n{query_url}')
            begin = time.time()
            transcript_res = self.session.get(query_url)
            transcript_json_response = transcript_res.json()
            if len(transcript_json_response) != 0:
                self.transcript_json.append(transcript_json_response)
            end = time.time()
            if self.console_output:
                print(f'''    {__name__} response and file write took:
    {round(end - begin, 4)} seconds.
    The program has ran for {round(time.time() - self.object_creation_time, 4)} seconds.
    rn: {rn}
    {__name__} Progress: {round(((len(self.contact_json) - rn) / len(self.contact_json)) * 100, 2)}%'''
                      )
            rn -= 1
        with open(os.path.join(self.temp_dir, file_name), 'a') as ab:
            ab.write(json.dumps(self.transcript_json))
        self._fix_file(os.path.join(self.temp_dir, file_name))

    def get_comments(self):
        file_name = 'comments' + self.file_type
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
    {__name__} Progress: {round(((len(self.eval_json) - rn) / len(self.eval_json)) * 100, 2)}%'''
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

    def _truncate_table(self, full_file_path):
        fn = os.path.basename(full_file_path)
        with open(os.path.join(self.sql_dir, 'truncate_table.sql')) as tf:
            self.sn.sql_command(tf.read().replace('<<tn>>', self.table_pairs[fn]))

    def _stage_file(self, full_file_path):
        with open(os.path.join(self.sql_dir, 'stage_file.sql'), 'r') as sf:
            query_text_raw = sf.read()
            query_text_raw = query_text_raw.replace('<<fp>>', full_file_path)
            query_text_final = query_text_raw.replace('<<ac>>', 'true')
            # Eventually turn the true/false value into a user defined dynamic field.
            if self.console_output:
                print(query_text_final)
            self.sn.sql_command(query_text_final)

    def _populate_table(self, full_file_path):
        fn = os.path.basename(full_file_path)
        with open(os.path.join(self.sql_dir, 'populate_table.sql')) as cf:
            copy_text_raw = cf.read()
            copy_text_raw = copy_text_raw.replace('<<tn>>', self.table_pairs[fn])
            copy_text_final = copy_text_raw.replace('<<pat>>', full_file_path)
            print(copy_text_final)
            self.sn.sql_command(copy_text_final)

    def run_table_updates(self):
        json_files = os.listdir(self.json_dir)
        new_file_list = []
        for file in json_files:
            if self.all_contacts_re.match(file):
                file_name = 'all_contacts*.json'
            else:
                file_name = file
            if file_name not in new_file_list:
                new_file_list.append(file_name)
        self.sn.sql_command('remove @my_uploader_stage')
        for f in new_file_list:
            file_path = os.path.join(self.json_dir, f)
            self._truncate_table(file_path)
            self._stage_file(file_path)
            self._populate_table(file_path)
        # self.sn.sql_command('remove @my_uploader_stage')

    def full_run(self, fun_list):
        """
        todo: Turn the loop into a way to run the functions is proper executing order.
        :param fun_list:
        :type fun_list:
        :return:
        :rtype:
        """
        self._remove_temp_files()
        if type(fun_list).__name__ == 'list':
            for fun in fun_list:
                if fun.__name__ == 'full_run' or fun.__name__ == 'run_table_updates':
                    print(f'{fun.__name__} was passed to avoid an infinite loop of recursion.')
                print(f'running function: {fun.__name__}')
                fun()
                print(f'function: {fun.__name__} ran with no errors.\n')
            self.run_table_updates()
            self._remove_temp_files()
            print(f'The {__name__} function ended after {round(time.time() - self.object_creation_time, 4)} seconds.')
        else:
            print(f'Sorry, a list of function objects needs to be passed into this function.\nType: {type(fun_list)}')

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
