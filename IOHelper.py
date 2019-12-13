from os import path, mkdir, rename, remove


def create_directory(dir_path: str):
    if not path.isdir(dir_path):
        mkdir(dir_path)


def move_file(from_path: str, to_path:str):
    rename(from_path, to_path)


def remove_file(file_path: str):
    remove(file_path)


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