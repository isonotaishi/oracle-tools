import sys
import traceback
import logging, logging.config
import yaml
import cx_Oracle
from getpass import getpass

class db_connect():
    '''DB接続用の情報をまとめたclass'''
    connect_identifier = ''
    username = ''
    password = ''

    def __init__(self, config):

        def get_username(cfg):
            return cfg['username']

        def get_hostname(cfg):
            return cfg['hostname']

        def get_port(cfg):
            return cfg['port']

        def get_service_name(cfg):
            return cfg['service_name']
        
        def get_instance_name(cfg):
            return cfg['instance_name']

        with open(config, 'r+') as cfg:
            connect_to = yaml.safe_load(cfg)

        try:
            self.connect_identifier = '{0}:{1}/{2}'.format(get_hostname(connect_to), get_port(connect_to), get_service_name(connect_to))
            if 'instance_name' in connect_to:
                self.connect_identifier += '/' + get_instance_name(connect_to)
                
            self.username = get_username(connect_to)
            self.password = getpass('input password for oracle account: ')

        except KeyError as e:
            print(traceback.format_exc())
            print('[ERROR] config file has not enough key, parsing config is stopped at: ', e)
            sys.exit()

    def test_connect(self):
        try:
            with cx_Oracle.connect(self.username, self.password, self.connect_identifier) as conn:
                with conn.cursor() as cur:
                    cur.execute('''select 1 from dual''')
        except Exception as e:
            '''失敗原因の切り分けのルーチン'''
            print(e)

'''logging関連のfunc'''