#!/usr/bin/python3
# -*- coding: UTF-8 -*-
"""
desc: process commands for magicdb-cli
author: TimePi
"""
from magicdb.magicdbListenerHandler import parse, set_engine_namespace
from magicdb.magicdbEtcdClient import MagicDBEtcdClient
import argparse
import sys
import etcd3


def process(namespace: str, host: str, port: int, passwd: str = None):
    etcd_client = MagicDBEtcdClient(namespace, host, port, passwd)
    print("magicdb> ", end="", flush=True)
    command = ""
    while True:
        try:
            line = sys.stdin.readline()
            line = line.strip()
            command += " " + line
            if line.endswith(";"):
                command = command.strip()
                if command == "exit;":
                    break
                parse(command, etcd_client)
                print("magicdb> ", end="", flush=True)
                command = ""
            else:
                print("magicdb> ", end="", flush=True)
        except etcd3.exceptions.Etcd3Exception:
            print("Etcd Exception Occur, Exit")
            exit(1)
        except:
            print("magicdb> ", end="", flush=True)
            command = ""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str, required=True, help="etch host")
    parser.add_argument("--port", type=int, help="etcd port", default=2379)
    parser.add_argument("--password", type=str,
                        help="etcd password", default=None)
    parser.add_argument("--name", type=str, required=True, help="namespace")
    args = parser.parse_args()
    set_engine_namespace(args.name)
    host, port, passwd = args.host, args.port, args.password
    process(args.name, host, port, passwd)


if __name__ == "__main__":
    main()
