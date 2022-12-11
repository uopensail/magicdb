#!/usr/bin/python3
# -*- coding: UTF-8 -*-
"""
desc: 
author: rand
"""
import json
import etcd3


class MagicDBEtcdClient:
    def __init__(self, name: str, host: str, port: int, passwd: str = None) -> None:
        self.client = etcd3.Etcd3Client(host=host, port=port, password=passwd)
        self.name = name
        self.engine_prefix = f'{self.name}'
        self.db_prefix = f'{self.name}/databases'
        self.table_prefix = f'{self.name}/databases/tables'
        self.machine_prefix = f'{self.name}/databases/machines'
        self.version_prefix = f'{self.name}/databases/versions'
        self.current_version_prefix = f'{self.name}/databases/current_versions'

    def db_key(self, database: str):
        return f'/{self.db_prefix}/{database}'

    def machine_key(self, machine: str):
        return f'/{self.machine_prefix}/{machine}'

    def table_key(self, database: str, table: str):
        return f'/{self.table_prefix}/{database}/{table}'

    def current_version_key(self, database: str, table: str):
        return f'/{self.current_version_prefix}/{database}/{table}'

    def version_key(self, database: str, table: str, version: str):
        return f'/{self.version_prefix}/{database}/{table}/{version}'

    def get_engine_info(self):
        value, _ = self.client.get(self.engine_prefix)
        if value is None:
            return {}
        else:
            return json.loads(value)

    def check_database(self, database: str):
        resp = self.client.get_response(key=self.db_key(database))
        return resp.count >= 1

    def create_database(self, database: str, properties: dict) -> bool:
        assert ('access_key' in properties
                and 'secret_key' in properties
                and 'bucket' in properties
                and 'endpoint' in properties
                and 'plaform' in properties)
        values = json.dumps(
            {'properties': properties, 'machines': [], 'tables': []})
        status = True
        with self.client.lock(self.name, ttl=10):
            if self.check_database(database):
                print(f'database: {database} exists')
                status = False
            else:
                self.client.put(key=self.db_key(database), value=values)
                info = self.get_engine_info()
                if 'databases' not in info:
                    info['databases'] = []
                if database not in info.get('databases', []):
                    info['databases'].append(database)
                self.client.put(self.engine_prefix, json.dumps(info))
        return status

    def drop_database(self, database: str) -> bool:
        db_key = self.db_key(database)
        table_key = f'/{self.table_prefix}/{database}'
        version_key = f'/{self.version_prefix}/{database}'
        current_version_key = f'/{self.current_version_prefix}/{database}'
        status = True
        with self.client.lock(self.name, ttl=10):
            if not self.check_database(database):
                print(f'database:`{database}` not exists')
                status = False
            else:
                info = self.get_db_info(database)
                for machine in info.get('machines', []):
                    self.client.delete_prefix(
                        prefix=f'/{self.machine_prefix}/{machine}')
                self.client.delete_prefix(prefix=current_version_key)
                self.client.delete_prefix(prefix=version_key)
                self.client.delete_prefix(prefix=table_key)
                self.client.delete_prefix(prefix=db_key)
        return status

    def get_db_info(self, database: str):
        if not self.check_database(database):
            print(f'database:`{database}` not exists')
            return {}
        value, _ = self.client.get(self.db_key(database))
        if value is not None:
            return json.loads(value)
        return {}

    def show_databases(self):
        info = self.get_engine_info()
        return info.get('databases', [])

    def check_machine(self,  database: str, machine: str):
        if not self.check_database(database):
            print(f'database:`{database}` not exists')
            return False
        resp = self.client.get_response(
            key=self.machine_key(machine))
        return resp.count >= 1

    def show_machines(self, database: str):
        info = self.get_db_info(database)
        return info.get('machines', [])

    def add_machine(self, database: str, machine: str):
        key = f'/{self.machine_prefix}/{machine}'
        status = True
        with self.client.lock(self.name, ttl=10):
            if not self.check_database(database):
                print(f'database:`{database}` not exists')
                status = False
            else:
                self.client.put(key=key, value=json.dumps({'db': database}))
                info = self.get_db_info(database)
                if machine not in info['machines']:
                    info['machines'].append(machine)
                self.client.put(self.db_key(database), json.dumps(info))
        return status

    def delete_machine(self, database: str, machine: str):
        key = f'/{self.machine_prefix}/{machine}'
        status = True
        with self.client.lock(self.name, ttl=10):
            if not self.check_machine(database, machine):
                print(f'database:`{database}` machine: `{machine}` not exists')
                status = False
            else:
                self.client.delete(key)
                info = self.get_db_info(database)
                if machine in info['machines']:
                    info['machines'].remove(machine)
                self.client.put(self.db_key(database), json.dumps(info))
        return status

    def check_table(self,  database: str, table: str):
        if not self.check_database(database):
            print(f'database:`{database}` not exists')
            return False
        resp = self.client.get_response(key=self.table_key(database, table))
        return resp.count >= 1

    def drop_table(self, database: str, table: str):
        table_key = self.table_key(database, table)
        version_key = f'/{self.version_prefix}/{database}/{table}'
        current_version_key = f'/{self.current_version_prefix}/{database}/{table}'
        status = True
        with self.client.lock(self.name, ttl=10):
            if not self.check_table(database, table):
                print(f'table:`{database}.{table}` not exists')
                status = False
            else:
                self.client.delete_prefix(prefix=current_version_key)
                self.client.delete_prefix(prefix=version_key)
                self.client.delete_prefix(prefix=table_key)
                info = self.get_db_info(database)
                if table in info['tables']:
                    info['tables'].remove(table)
                self.client.put(self.db_key(database), json.dumps(info))
        return status

    def get_table_info(self, database: str, table: str):
        if not self.check_table(database, table):
            print(f'table:`{database}.{table}` not exists')
            return {}
        value, _ = self.client.get(self.table_key(database, table))
        if value is not None:
            return json.loads(value)
        return {}

    def show_tables(self, database: str):
        info = self.get_db_info(database)
        return info.get('tables', [])

    def create_table(self, database: str, table: str, properties: dict):
        assert ('data_path' in properties and 'meta_path' in properties)
        status = True
        with self.client.lock(self.name, ttl=10):
            if self.check_table(database, table):
                print(f'table:`{database}.{table}` exists')
                status = False
            elif not self.check_database(database):
                print(f'database:`{database}` not exists')
                status = False
            else:
                self.client.put(key=self.table_key(database, table),
                                value=json.dumps({'properties': properties, 'db': database, 'versions': [],
                                                  'current_version': 'nil'}))
                info = self.get_db_info(database)
                if table not in info['tables']:
                    info['tables'].append(table)
                self.client.put(self.db_key(database), json.dumps(info))
        return status

    def show_versions(self, database: str, table: str):
        info = self.get_table_info(database, table)
        return info.get('versions', [])

    def show_current_version(self, database: str, table: str):
        info = self.get_table_info(database, table)
        return info.get('current_version', 'nil')

    def add_version(self,  database: str, table: str, version: str):
        status = True
        with self.client.lock(self.name, ttl=10):
            if not self.check_table(database, table):
                print(f'table:`{database}.{table}` not exists')
                status = False
            else:
                self.client.put(key=self.version_key(
                    database, table, version), value=version)
                table_info = self.get_table_info(database, table)
                if version not in table_info['versions']:
                    table_info['versions'].append(version)
                self.client.put(self.table_key(database, table),
                                json.dumps(table_info))
        return status

    def check_version(self,  database: str, table: str, version: str):
        if not self.check_table(database, table):
            print(f'table:`{database}.{table}` not exists')
            return False
        resp = self.client.get_response(
            key=self.version_key(database, table, version))
        return resp.count >= 1

    def update_current_version(self, database: str, table: str, version: str):
        status = True
        with self.client.lock(self.name, ttl=10):
            if not self.check_version(database, table, version):
                print(
                    f'table:`{database}.{table}` version:`{version}` not exists')
                status = False
            else:
                table_info = self.get_table_info(database, table)
                self.client.put(self.current_version_key(
                    database, table), version)
                table_info['current_version'] = version
                self.client.put(self.table_key(
                    database, table), json.dumps(table_info))
        return status

    def drop_version(self, database: str, table: str, version: str):
        status = True
        with self.client.lock(self.name, ttl=10):
            if not self.check_version(database, table, version):
                print(
                    f'table:`{database}.{table}` version:`{version}` not exists')
                status = False
            else:
                self.client.delete(key=self.version_key(
                    database, table, version))
                table_info = self.get_table_info(database, table)
                if version in table_info['versions']:
                    table_info['versions'].remove(version)
                if version == table_info['current_version']:
                    table_info['current_version'] = 'nil'
                self.client.put(self.table_key(
                    database, table), json.dumps(table_info))
        return status
