from airflow.models import BaseOperator
from notebook_plugin.hooks.git_hook import GitHook
from airflow.utils.decorators import apply_defaults

import shutil

class NotebookToGitOperator(BaseOperator):

    template_fields = ('nb_name',)

    @apply_defaults
    def __init__(self, nb_path, nb_name, conn_id=None, *args, **kwargs):
      super(NotebookToGitOperator, self).__init__(*args, **kwargs)
      self.conn_id  = conn_id
      self.nb_path  = nb_path
      self.nb_name  = nb_name

    def execute(self, context):
        self.hook = GitHook(self.conn_id)
        self.log.info("Copying the notebook {0} in the repo".format(self.nb_name))
        shutil.copyfile(src='{0}/{1}'.format(self.nb_path, self.nb_name), 
                        dst='{0}/{1}-{2}'.format(self.hook.path, context['execution_date'], self.nb_name))
        self.log.info("Pushing the notebook in the repo")
        self.hook.push()
