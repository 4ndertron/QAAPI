from . import dt
from . import os
from . import project_dir


class Butler:
    @staticmethod
    def freeze_env(base_dir=None):
        if base_dir is None:
            freeze_base_dir = project_dir
        else:
            freeze_base_dir = base_dir
        save_date = dt.date.today()
        requirements_name = f'{os.environ["conda_default_env"]}_requirements_{save_date.strftime("%Y_%m_%d")}.txt'
        os.system(
            f'pip freeze > {os.path.join(freeze_base_dir, requirements_name)}')
        return {'file_name': requirements_name, 'base_dir': freeze_base_dir}
