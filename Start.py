from modules.qa_api_handler import ApiHandler
import os
import re
import datetime as dt
import threading
import time

ignore_re = re.compile('^_?|all_contacts|full_run')
date_format = '%Y-%m-%d'
query_file = os.path.join(ApiHandler.sql_dir)
end = dt.date.today()


def all_contacts():
    """
    Create an api object for collecting all of the contacts.
    """
    # begin_all = dt.date.fromisoformat('2019-03-01')
    begin_all = dt.date.today() + dt.timedelta(-8)

    api_contacts = ApiHandler(
        console_output=True,
        schema='d_post_install',
        all_contacts=True,
        begin_date=begin_all.strftime(date_format),
        end_date=end.strftime(date_format),
        break_size=2,
    )
    all_fun_list = [
        api_contacts.get_all_contacts,
    ]
    api_contacts.full_run(fun_list=all_fun_list)


def qa_contacts():
    """
    Create an api object for collecting just the qa contacts.
    """
    begin_qa = dt.date.today() + dt.timedelta(-30)
    api_qa = ApiHandler(
        console_output=True,
        schema='d_post_install',
        all_contacts=False,
        begin_date=begin_qa.strftime(date_format),
        end_date=end.strftime(date_format),
        break_size=(end - begin_qa).days
        # sql_file=query_file
    )
    qa_fun_list = [
        api_qa.get_forms(),
        api_qa.get_all_contacts(),
        api_qa.get_evaluations(),
        api_qa.get_comments()
    ]
    api_qa.full_run(qa_fun_list)


def table_update_only():
    table_api = ApiHandler(
        console_output=True,
        schema='d_post_install',
        begin_date=dt.date.today().strftime(date_format),
        end_date=end.strftime(date_format),
        break_size=1
    )
    table_api.run_table_updates()


def main():
    main_begin = time.time()
    all_contact_thread = threading.Thread(target=all_contacts, name='all_contacts')
    qa_contact_thread = threading.Thread(target=qa_contacts, name='qa_contacts')
    table_updates_thread = threading.Thread(target=table_update_only, name='table_updates')

    if dt.date.today().isoweekday() == 2:
        qa_contact_thread.start()
    all_contact_thread.start()
    # table_updates_thread.start()

    while threading.active_count() > 1:
        print(f'{threading.active_count() - 1} extra processes running.\n')
        print(f'program runtime = {round(time.time() - main_begin, 2)}')
        time.sleep(5)


if __name__ == '__main__':
    main()
