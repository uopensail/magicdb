#!/usr/bin/python3
# -*- coding: UTF-8 -*-
"""
desc: process commands for magicdb-cli
author: TimePi
"""
import sys
import argparse
from magicdbEtcdClient import MagicDBEtcdClient
from magicdbListenerHandler import parse


def main(name: str, host: str, port: int, passwd: str = None):
    etcd_client = MagicDBEtcdClient(name, host, port, passwd)
    print('>>> ', end='', flush=True)
    command = ''
    while True:
        try:
            line = sys.stdin.readline()
            line = line.strip()
            command += ' '+line
            if line.endswith(';'):
                command = command.strip()
                if command == 'exit;':
                    break
                parse(command, etcd_client)
                print('>>> ', end='', flush=True)
                command = ''
            else:
                print('... ', end='', flush=True)
        except:
            pass


if __name__ == '__main__':
    name, host, port, passwd = 'magicdb', 'localhost', 2379, None
    main(name, host, port, passwd)
