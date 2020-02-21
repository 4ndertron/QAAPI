import re
import os
import json
import time
import sqlite3 as sql
import datetime as dt
import requests as rq

project_dir = os.path.join(os.environ['userprofile'], 'PycharmProjects', 'QAAPI')
modules_dir = os.path.join(project_dir, 'modules')
queries_dir = os.path.join(project_dir, 'modules')
sql_templates_dir = os.path.join(project_dir, 'sql_templates')
db_dir = os.path.join(project_dir, 'local_db')
json_dir = os.path.join(project_dir, 'json')
errors_dir = os.path.join(project_dir, 'errors')
