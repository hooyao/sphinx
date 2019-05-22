#!/usr/bin/python3
import argparse
import logging
import multiprocessing
import os
import re
import subprocess
from itertools import repeat
from logging.handlers import WatchedFileHandler

DIR_NAME_PATTERN = re.compile(r"^(19[0-9]{2}|2[0-9]{3})(0[1-9]|1[012])([123]0|[012][1-9]|31)$")


class Extractor:
    _GZIP_PART_FILE_PREFIX = "sphinx_part_"
    _FILE_NAME_PATTERN = re.compile(r"^sphinx_part_\d{5}$")
    _SUC_FILE_NAME = "extract_complete.txt"
    _ERR_FILE_NAME = "extract_error.txt"
    _DIST_FILE_NAME = "dist_path.txt"

    def __init__(self, logger, source_dir, dest_dir, exts):
        self.logger = logger
        self.source_dir = source_dir
        self.dest_dir = dest_dir
        self.date_string = self.source_dir.split(os.sep)[-1]
        self.exts = exts
        if not os.path.exists(self.dest_dir):
            os.makedirs(self.dest_dir)

    def extract_files(self):
        anchor_file_list = [os.path.join(self.source_dir, f) for f in os.listdir(self.source_dir) if
                            os.path.isfile(os.path.join(self.source_dir, f))
                            and (self._SUC_FILE_NAME in f or self._DIST_FILE_NAME in f)]

        all_paths = [os.path.join(self.source_dir, f) for f in os.listdir(self.source_dir) if
                     os.path.isfile(os.path.join(self.source_dir, f)) and self._FILE_NAME_PATTERN.match(f)]
        # list(sorted(all_paths)))
        if len(anchor_file_list) == 2:
            self.logger.info(f'{self.date_string} is already handled, skip...')
        else:
            excludes = " ".join([f"--exclude=*{ext}" for ext in self.exts])
            cmd_line = f"cat {os.path.join(self.source_dir,self._GZIP_PART_FILE_PREFIX)}*|tar zx {excludes} " \
                       f"--strip-components=1 -C {self.dest_dir} "
            result = subprocess.run(cmd_line, shell=True, stderr=subprocess.PIPE)
            if result.returncode == 0:  # extract ok
                os.mknod(os.path.join(self.source_dir, self._SUC_FILE_NAME))
                with open(os.path.join(self.source_dir, self._DIST_FILE_NAME), "w") as f:
                    f.write(self.dest_dir)
                self.logger.info(
                    f'Extract files except {self.exts}in {self.source_dir} to {self.dest_dir} is complete.')
                return True
            else:
                os.mknod(os.path.join(self.source_dir, self._ERR_FILE_NAME))
                self.logger.info(
                    f'Extract files except {self.exts} in {self.source_dir} to {self.dest_dir} with error.')
                return False


def parse_args():
    parser = argparse.ArgumentParser(prog='Efapiao gzip parts fix program')
    parser.add_argument('-s', '--source', type=str, required=True,
                        help='The inventory of efapiao gzip part detail.')
    parser.add_argument('-d', '--dest', type=str, required=True,
                        help='The directory where extract files go.')
    parser.add_argument('-e', '--exclude', type=str, required=True,
                        help='The extensions to exclude,seperated by "," ')
    return parser.parse_args()


def setup_logger_handler():
    formatter = logging.Formatter("%(asctime)s - %(processName)s - %(name)s - %(levelname)s - %(message)s")

    cwd = os.getcwd()
    logg_file = os.path.join(cwd, "extractor.log")
    file_handler = WatchedFileHandler(logg_file)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    return file_handler, console_handler


def extract_for_one_day(extractor_logger, source_dest, exts_to_exclude):
    extractor = Extractor(extractor_logger, source_dest[0], source_dest[1], exts_to_exclude)
    result = extractor.extract_files()


def main():
    args = parse_args()

    source_dir = os.path.abspath(os.path.expanduser(args.source))
    dest_dir = os.path.abspath(os.path.expanduser(args.dest))
    exts_to_exlude = args.exclude.split(',')

    handlers = setup_logger_handler()
    main_logger = logging.getLogger("main")
    main_logger.setLevel("INFO")
    for handler in handlers:
        main_logger.addHandler(handler)

    extractor_logger = logging.getLogger("extractor")
    extractor_logger.setLevel("INFO")
    for handler in handlers:
        extractor_logger.addHandler(handler)

    all_dirs = [(os.path.join(source_dir, f), os.path.join(dest_dir, f)) for f in os.listdir(source_dir) if
                os.path.isdir(os.path.join(source_dir, f)) and DIR_NAME_PATTERN.match(f)]

    with multiprocessing.Pool(processes=3) as pool:
        results = pool.starmap(func=extract_for_one_day,
                               iterable=zip(repeat(extractor_logger),
                                            all_dirs,
                                            repeat(exts_to_exlude)),
                               chunksize=1)


if __name__ == '__main__':
    main()
