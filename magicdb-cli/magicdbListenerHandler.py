#!/usr/bin/python3
# -*- coding: UTF-8 -*-
"""
desc: process commands for magicdb
author: rand
"""
import antlr4
from magicdbLexer import magicdbLexer
from magicdbParser import magicdbParser
from magicdbListener import magicdbListener
from magicdbEtcdClient import MagicDBEtcdClient


class MagicDBListenerHandler(magicdbListener):
    def __init__(self, etcd_client: MagicDBEtcdClient) -> None:
        super().__init__()
        self.etcd_client = etcd_client
        self.stack = []

    def exitDrop_database(self, ctx: magicdbParser.Drop_databaseContext):
        db = ctx.database_name().getText()
        status = self.etcd_client.drop_database(db)
        print(f'drop db {db} success' if status
              else f'drop db {db} fail')

    def exitShow_databases(self, ctx: magicdbParser.Show_databasesContext):
        databases = self.etcd_client.show_databases()
        print('database list: ')
        print('['+'\n'.join(map(lambda _: f'`{_}`', databases)) + ']')

    def exitCreate_database(self, ctx: magicdbParser.Create_databaseContext):
        db = ctx.database_name().getText()
        properties = self.stack.pop(-1)
        status = self.etcd_client.create_database(db, properties)
        print(f'create db {db} success' if status
              else f'create db {db} fail')

    def exitShow_machines(self, ctx: magicdbParser.Show_machinesContext):
        database = ctx.database_name().getText()
        machines = self.etcd_client.show_machines(database)
        print('machine list: ')
        print('['+'\n'.join(map(lambda _: f'`{_}`', machines))+']')

    def exitDelete_machine(self, ctx: magicdbParser.Delete_machineContext):
        db = ctx.database_name().getText()
        machine = ctx.STRING().getText()[1:-1]
        status = self.etcd_client.delete_machine(db, machine)
        print(f'drop machine `{machine}` success' if status
              else f'drop machine `{machine}` fail')

    def exitAdd_machine(self, ctx: magicdbParser.Add_machineContext):
        database = ctx.database_name().getText()
        machine = ctx.STRING().getText()[1:-1]
        status = self.etcd_client.add_machine(database, machine)
        print(f'add machine `{machine}` success' if status
              else f'add machine `{machine}` fail')

    def exitShow_tables(self, ctx: magicdbParser.Show_tablesContext):
        database = ctx.database_name().getText()
        tables = self.etcd_client.show_tables(database)
        print('table list: ')
        print('['+'\n'.join(map(lambda _: f'`{_}`', tables))+']')

    def exitDrop_table(self, ctx: magicdbParser.Drop_tableContext):
        table_str = ctx.table().getText()
        items = table_str.split('.')
        database, table = items[0], items[1]
        status = self.etcd_client.drop_table(database, table)
        print(f'drop table `{table}` success' if status
              else f'drop table `{table}` fail')

    def exitCreate_table(self, ctx: magicdbParser.Create_tableContext):
        table_str = ctx.table().getText()
        items = table_str.split('.')
        db, table = items[0], items[1]
        properties = self.stack.pop(-1)
        status = self.etcd_client.create_table(db, table, properties)
        print(f'create table `{table}` success' if status
              else f'create table `{table}` fail')

    def exitDesc_table(self, ctx: magicdbParser.Desc_tableContext):
        table_str = ctx.table().getText()
        items = table_str.split('.')
        database, table = items[0], items[1]
        print(self.etcd_client.get_table_info(database, table))

    def exitShow_versions(self, ctx: magicdbParser.Show_versionsContext):
        table_str = ctx.table().getText()
        items = table_str.split('.')
        database, table = items[0], items[1]
        versions = self.etcd_client.show_versions(database, table)
        print('version list: ')
        print('['+'\n'.join(map(lambda _: f'`{_}`', versions))+']')

    def exitShow_current_version(self, ctx: magicdbParser.Show_current_versionContext):
        table_str = ctx.table().getText()
        items = table_str.split('.')
        database, table = items[0], items[1]
        version = self.etcd_client.show_current_version(database, table)
        print(f'current version: `{version}`')

    def exitUpdate_version(self, ctx: magicdbParser.Update_versionContext):
        table_str = ctx.table().getText()
        items = table_str.split('.')
        database, table = items[0], items[1]
        version = ctx.STRING().getText()[1:-1]
        self.etcd_client.update_version(database, table, version)
        current_version = self.etcd_client.show_current_version(
            database, table, version)
        print(f'current version: `{current_version}`')

    def exitDrop_version(self, ctx: magicdbParser.Drop_versionContext):
        table_str = ctx.table().getText()
        items = table_str.split('.')
        database, table = items[0], items[1]
        version = ctx.STRING().getText()[1:-1]
        print(database, table, version)
        self.etcd_client.drop_version(database, table, version)

    def exitLoad_data(self, ctx: magicdbParser.Load_dataContext):
        table_str = ctx.table().getText()
        items = table_str.split('.')
        database, table = items[0], items[1]
        remote_path = ctx.STRING().getText()
        properties = self.stack.pop(-1)
        print(database, table, remote_path, properties)
        self.etcd_client.add_version(database, table, "version1")
        self.etcd_client.update_current_version(database, table, "version1")

    def exitSelect_data(self, ctx: magicdbParser.Select_dataContext):
        table_str = ctx.table().getText()
        items = table_str.split('.')
        database, table = items[0], items[1]
        key = ctx.STRING().getText()
        print(database, table, key)

    def exitProperties(self, ctx: magicdbParser.PropertiesContext):
        properties = {}
        while len(self.stack) > 0:
            pair = self.stack.pop(-1)
            properties.update(pair)
        self.stack.append(properties)
        return properties

    def exitPair(self, ctx: magicdbParser.PairContext):
        key = ctx.STRING().getText()
        value = ctx.value().getText()
        pair = eval('{%s:%s}' % (key, value))
        self.stack.append(pair)
        return pair


def parse(command: str, etcd_client: MagicDBEtcdClient):
    lexer = magicdbLexer(antlr4.InputStream(command))
    stream = antlr4.CommonTokenStream(lexer)
    parser = magicdbParser(stream)
    walker = antlr4.ParseTreeWalker()
    tree = parser.start()
    client = MagicDBListenerHandler(etcd_client)
    walker.walk(client, tree)


if __name__ == '__main__':
    drop_database = 'drop database if exists database1;'
    create_database = 'create database if not exists database1 with properties("access_key" = "access_key","secret_key" = "secret_key","bucket"="bucket","endpoint"="endpoint","plaform"="plaform");'
    show_databases = 'show databases;'
    add_machine = 'alter database database1 add machine("10.0.0.3");'
    show_machines = 'show machines database1;'
    del_machine_1 = 'alter database database1 drop machine("10.0.0.4");'
    del_machine_2 = 'alter database database1 drop machine("10.0.0.3");'
    show_tables_1 = 'show tables database1;'
    show_tables_2 = 'show tables database2;'
    create_table = 'create table database1.table1 with properties("data_path"="data_path","meta_path"="meta_path");'
    drop_table = 'drop table if exists database1.table1;'

    show_versions_1 = 'show versions database1.table1;'
    show_versions_2 = 'show versions database1.table2;'
    show_current_versions = 'show current version database1.table1;'
    load_data = 'load data "oss://xxxx/dt=20221124/" into table database1.table1 with PROPERTIES("k1" = "v1","k2" = "v2");'

    drop_version_1 = 'alter table database1.table1 drop version("version1");'
    drop_version_2 = 'alter table database1.table1 drop version("version2");'
    desc_table = 'desc database1.table1;'
    etcd_client = MagicDBEtcdClient('magicdb', 'localhost', 2379)

    parse(drop_database, etcd_client)
    parse(show_databases, etcd_client)
    parse(create_database, etcd_client)
    parse(create_database, etcd_client)
    parse(show_databases, etcd_client)
    parse(show_machines, etcd_client)
    parse(add_machine, etcd_client)
    parse(show_machines, etcd_client)
    parse(del_machine_1, etcd_client)
    parse(del_machine_1, etcd_client)
    parse(del_machine_2, etcd_client)
    parse(show_machines, etcd_client)
    parse(show_tables_1, etcd_client)
    parse(show_tables_2, etcd_client)
    parse(create_table, etcd_client)
    parse(show_tables_1, etcd_client)

    # parse(drop_table, etcd_client)
    parse(show_versions_1, etcd_client)
    parse(show_versions_2, etcd_client)
    parse(show_current_versions, etcd_client)
    parse(load_data, etcd_client)
    parse(show_current_versions, etcd_client)
    parse(show_versions_1, etcd_client)

    parse(drop_version_2, etcd_client)
    parse(drop_version_1, etcd_client)
    parse(show_databases, etcd_client)
    parse(desc_table, etcd_client)
