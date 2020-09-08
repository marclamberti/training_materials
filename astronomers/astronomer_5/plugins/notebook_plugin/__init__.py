from airflow.plugins_manager import AirflowPlugin
from notebook_plugin.operators.notebook_to_keep_operator import NotebookToKeepOperator
from notebook_plugin.operators.notebook_to_git_operator import NotebookToGitOperator
from notebook_plugin.hooks.git_hook import GitHook

class AirflowNotebookPlugin(AirflowPlugin):
    name = "notebook_plugin"

    operators = [NotebookToKeepOperator, NotebookToGitOperator]
    hooks = [GitHook]