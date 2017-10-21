#! /usr/bin/env python
# coding: utf8

import sys
import os
import optparse
import fcntl
import functools
import tempfile
import signal
import json
import time
import shutil
import traceback
import logging
import logging.config
import concurrent.futures

QUIT = False
LOGGER = logging.getLogger()

def on_quit(sn, fo):
    global QUIT
    QUIT = True
    LOGGER.info("process: %d receives"
        " signal: %d, preparing to quit" % (os.getpid(), sn))

def handle_signal():
    signal.signal(signal.SIGTERM, on_quit)
    signal.signal(signal.SIGINT, on_quit)
    signal.signal(signal.SIGHUP, on_quit)
    signal.signal(signal.SIGUSR1, on_quit)
    signal.signal(signal.SIGUSR2, on_quit)

def is_valid_file(filename, mode=1):
    if not os.path.isfile(filename):
        return "%s is not a valid file" % filename

    if mode == 2 and os.access(filename, os.W_OK):
        return None
    elif mode == 2:
        return "no write permission for file: %s" % filename
    elif not os.access(filename, os.R_OK):
        return "no read permission for file: %s" % filename
    return None

def handle_command_line_options(args):
    parser = optparse.OptionParser("Usage: %prog [options]")
    parser.add_option("-f", "--logfile", dest="logfile",
        help="log file")
    parser.add_option("-l", "--lock-file", dest="lockfile",
        help="lock file")
    parser.add_option("-p", "--position-file", dest="position_file",
        help="position file")
    parser.add_option("-c", "--callback", dest="callback",
        help="callback of one line")
    parser.add_option("-t", "--think-time", dest="think_time",
        type=float, default=0.0, help="think time")
    parser.add_option("-e", "--encoding", dest="encoding",
        type=str, default="utf8", help="output encoding")
    parser.add_option("-g", "--logging-conf", dest="logging_conf",
        type=str, default="logging.conf", help="logging config file")
    parser.add_option("-w", "--worker-count", dest="worker_count",
        type=int, help="worker count")

    options, _ = parser.parse_args(args)
    if not options.logfile or \
            not options.lockfile or \
            not options.position_file:
        parser.error("no (log file/lock file/position file) provided")

    error_message = is_valid_file(options.logfile)
    if error_message:
        parser.error(error_message)
    error_message = is_valid_file(options.lockfile, 2)
    if error_message:
        parser.error(error_message)

    if options.callback:
        package_info, function_name = options.callback.split(":", 1)
        package_path = os.path.dirname(package_info)
        if package_path:
            sys.path.append(package_path)
        package_name = os.path.basename(package_info)
        options.callback = getattr(__import__(package_name), function_name)

    return options

def write_position(position_file, log_inode, position):
    temp_fd = tempfile.NamedTemporaryFile(prefix="etailf:", delete=False)
    temp_fd.write("%d\t%d" % (log_inode, position))
    temp_fd.close()
    os.rename(temp_fd.name, position_file)
    return position

def initialize_from_position_file(log_file_stat, position_file):
    log_inode = log_file_stat.st_ino
    if not os.path.isfile(position_file):
        return write_position(position_file, log_inode, 0)

    if not os.access(position_file, os.R_OK) or \
            not os.access(position_file, os.W_OK):
        raise IOError("permission denied for file: %s" % position_file)

    with open(position_file) as fd:
        try:
            inode, position = map(int, fd.read().strip().split("\t", 1))
        except Exception as ex:
            LOGGER.error("invalid content of position file: %s" % str(ex))
            return write_position(position_file, log_inode, 0)

    if inode != log_inode:
        LOGGER.info("file is already recovered")
        return write_position(position_file, log_inode, 0)
    if position > log_file_stat.st_size:
        LOGGER.info("log file is truncated")
        return write_position(position_file, log_inode,
            log_file_stat.st_size-1)
    return position

def read_file(logfile, position_file,
        think_time=0., callback=None, encoding="utf8", worker_count=None):
    global QUIT
    log_file_stat = os.stat(logfile)
    log_inode = log_file_stat.st_ino
    log_fd = open(logfile)
    position = initialize_from_position_file(log_file_stat,
        position_file)
    LOGGER.info("fetch data from position: %d" % position)
    log_fd.seek(position)

    executor = concurrent.futures.ProcessPoolExecutor(worker_count)
    futures = []

    count = 0
    while not QUIT:
        if think_time:
            time.sleep(think_time)

        one_line = log_fd.readline()
        position = position + len(one_line)
        count = count + 1
        if count % 100 == 0:
            write_position(position_file, log_inode, position)
            LOGGER.debug("%s lines have been processed" % count)

        if not one_line:
            try:
                log_inode_new = os.stat(logfile).st_ino
            except OSError as ex:
                if ex.errno == 2:
                    LOGGER.error("log file is missing! sleep 1s")
                    time.sleep(1)
                    continue
            if log_inode_new != log_inode:
                LOGGER.info("log file is rotated")
                log_fd.close()
                log_inode = os.stat(logfile).st_ino
                log_fd = open(logfile)
                position = write_position(position_file, log_inode, 0)
                continue
            time.sleep(0.01)
            continue

        if callable(callback):
            futures.append(executor.submit(callback, one_line))
            process_futures(futures, encoding)
            continue
        sys.stdout.write(one_line)

    log_fd.close()
    write_position(position_file, log_inode, position)
    executor.shutdown()
    LOGGER.info("futures count is: %s" % len(futures))
    process_futures(futures, encoding)
    LOGGER.info("shutdown end, futures count is: %s" % len(futures))

def process_futures(futures, encoding):
    index = 0
    while index < len(futures):
        future = futures[index]
        if not future.done():
            index = index + 1
            continue

        futures.pop(index)
        exception = future.exception()
        if exception:
            LOGGER.error(str(exception))
            continue

        parsed_line = future.result()
        if not parsed_line:
            continue
        try:
            if isinstance(parsed_line, unicode):
                parsed_line = parsed_line.encode(encoding)
            sys.stdout.write(parsed_line)
        except Exception as ex:
            LOGGER.error(str(ex))

def main(args=sys.argv[1:]):
    options = handle_command_line_options(args)
    if options.logging_conf and \
            is_valid_file(options.logging_conf) is None:
        logging.config.fileConfig(options.logging_conf)
    else:
        logging.basicConfig(level=logging.DEBUG,
            format="\033[31m%(asctime)s|%(name)-10s|" \
                "%(lineno)-5d|%(process)-5d|%(message)s\033[0m",
            datefmt="%F %T")

    handle_signal()
    lock_fd = open(options.lockfile, "w+")
    try:
        fcntl.lockf(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError as ex:
        if ex.errno == 11:
            LOGGER.error("another process is locking, exited")
            lock_fd.close()
            os._exit(1)

    LOGGER.info("got lock successfully")
    try:
        read_file(options.logfile, options.position_file, 
            options.think_time, options.callback, options.encoding,
            options.worker_count)
    finally:
        lock_fd.close()
    LOGGER.info("exit successfully")

if __name__ == "__main__":
    main()

