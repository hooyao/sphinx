#!/usr/bin/env python  
""" 
@author:hooyao
@license: Apache Licence 
@file: logging_config.py 
@time: 2019/05/22
@contact: hooyao@gmail.com
@site:  
@software: PyCharm 
"""
import logging
from logging import StreamHandler
from logging.handlers import WatchedFileHandler

formatter = logging.Formatter("%(asctime)s - %(processName)s - %(name)s - %(levelname)s - %(message)s")


def get_logging_handlers(logfile_path) -> (WatchedFileHandler, StreamHandler):
    file_handler = WatchedFileHandler(logfile_path)
    file_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    return file_handler, console_handler
