import os
import logging

def get_logger(file_path, log_path = './logs/aqua_api.log'):
    module_name = file_path.split('/')[-1].split('.')[0]
    #set the root logger to a debug level
    logger = logging.getLogger(module_name)
    logger.setLevel(logging.DEBUG)
    logger.handlers = []
    #set up a file handler and a stream handler
    #makes sure the log_path exists before calling it
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    f_handler = logging.FileHandler(log_path)
    s_handler = logging.StreamHandler()
    f_handler.setLevel(logging.INFO)
    s_handler.setLevel(logging.DEBUG)
    log_format = logging.Formatter(fmt='%(asctime)s | %(levelname)s | %(message)s | %(name)s',
                                 datefmt='%Y-%m-%d %H:%M:%S')
    f_handler.setFormatter(log_format)
    s_handler.setFormatter(log_format)
    logger.addHandler(f_handler)
    logger.addHandler(s_handler)
    return logger
