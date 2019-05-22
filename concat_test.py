#!/usr/bin/python3
import os
import tarfile
from io import IOBase
from os import path


class ConcatenatedFiles(IOBase):
    def __init__(self, file_list):
        self.file_list = []
        self.file_name_list = file_list
        self.file_len = 0
        self.pos = 0
        self.name = str(file_list)
        self.cur_file = 0

    def __enter__(self):
        for idx, file in enumerate(self.file_name_list):
            file_size = path.getsize(file)
            self.file_len += file_size
            f = open(file, "rb")
            self.file_list.append((f, file_size))
        self.pos = 0
        return self

    def read(self, size=-1):
        data = bytearray()
        bytes_left = size
        if size > 0:
            f = self.file_list[self.cur_file][0]
            while bytes_left > 0:
                buf = f.read(size)
                if len(buf) == 0:
                    if self.cur_file <= len(self.file_list) - 1:
                        self.cur_file += 1
                        self.pos += 1
                    else:
                        # reach the end of all files
                        data.extend(buf)
                        self.pos = self.file_len
                        break
                else:
                    if len(buf) <= size:
                        if self.cur_file <= len(self.file_list) - 1:
                            self.cur_file += 1
                            data.extend(buf)
                            bytes_left -= len(buf)
                        else:
                            data.extend(buf)
                            break
                    else:
                        data.extend(buf)
                        bytes_left -= len(buf)
        elif size < 0:
            raise IOError("We can't read all data from all the files.")
        return data

    def seek(self, pos, whence=0):
        if whence == 0:
            seek_left = pos
            for idx, finfo in enumerate(self.file_list):
                f = finfo[0]
                size = finfo[1]
                if seek_left < size:
                    f.seek(seek_left)
                    self.cur_file = idx
                    break
                else:
                    seek_left -= size
            self.pos = pos
        else:
            raise IOError("We don't support whence > 0.")

    def tell(self) -> int:
        return self.pos

    def __exit__(self, type, value, traceback):
        for f in self.file_list:
            f.close()


target_path = "./2M"
all_paths = [os.path.join(target_path, f) for f in os.listdir(target_path) if
             os.path.isfile(os.path.join(target_path, f))]
with ConcatenatedFiles(sorted(all_paths)) as f:
    with tarfile.open(fileobj=f)  as tar_f:
        mems = tar_f.getmembers()
        print(mems)
