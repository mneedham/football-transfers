"""
Various IO operations, e.g. parallel writing to file, removal of directories etc.

Written by Jan Gerling
"""

from os import path, mkdir, rename, remove
from shutil import rmtree


def create_directory(dir_path: str):
    if not path.isdir(dir_path):
        mkdir(dir_path)


def move_file(from_path: str, to_path:str):
    rename(from_path, to_path)


def remove_file(file_path: str):
    remove(file_path)


def remove_directory(path: str):
    rmtree(path, ignore_errors=True)


def parallel_write(q, file_path):
    first = True
    with open(file_path, 'w+', encoding='utf-8') as file:
        while True:
            val = q.get()
            if val is None:
                break
            if first and val.partition('\n')[0] == ",":
                first = False
                val = '\n'.join(val.partition('\n')[1:])
            file.write(val)
            q.task_done()
        q.task_done()