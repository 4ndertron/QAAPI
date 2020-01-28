import os


class Butler:
    @staticmethod
    def freeze_env(base_dir=None):
        if base_dir is None:
            freeze_base_dir = os.path.join(os.environ['userprofile'], 'downloads')
        else:
            freeze_base_dir = base_dir
        requirements_name = f'{os.environ["conda_default_env"]}_requirements.txt'
        os.system(
            f'pip freeze > {os.path.join(freeze_base_dir, requirements_name)}')
        return {'file_name': requirements_name, 'base_dir': freeze_base_dir}
