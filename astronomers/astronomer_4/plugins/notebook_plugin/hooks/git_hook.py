from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException

from git import Repo
import os

class GitHook(BaseHook):

    def __init__(self, conn_id='git_default'):
        conn = self.get_connection(conn_id)

        conn_config = {}

        if conn.host:
            conn_config['host'] = conn.host
        if conn.login:
            conn_config['login'] = conn.login

        conn_config['repo'] = conn.extra_dejson.get('repo', None)
        conn_config['oauth'] = conn.extra_dejson.get('oauth', None)
        conn_config['path'] = conn.extra_dejson.get('path', None)

        url = "https:///{0}:x-oauth-basic@{1}/{2}/{3}".format(conn_config['oauth'], conn_config['host'], conn_config['login'], conn_config['repo'])
        self.path = conn_config['path']
        isdir = os.path.isdir(self.path)
        self.repo = Repo(self.path) if isdir else Repo.clone_from(url, self.path)
        self.repo.config_writer().set_value("user", "name", conn_config['login']).release()
        self.repo.config_writer().set_value("user", "email", "myemail@astronomer.io").release()

    def push(self, commit='New commit from Airflow'):
        self.repo.remotes.origin.pull()
        self.repo.git.add(all=True)
        self.repo.git.commit('-m', commit)
        self.repo.remotes.origin.push()