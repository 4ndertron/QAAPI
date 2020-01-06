from modules.qa_api_handler import ApiHandler


def main():
    api = ApiHandler(console_output=True, schema='d_post_install')
    api.full_run()


if __name__ == '__main__':
    main()