#! /usr/bin/python
# -*- coding: utf-8 -*-
"""
script: winfates_mp.py
Created on Fri May 10 18:43:19 2013
@author: gav
Description:

"""
from __future__ import print_function, division
import os
import multiprocessing
import subprocess
import argparse
import time
from random import randint
from collections import namedtuple
import fnmatch
# import psutil
import logging
logging.basicConfig(level=logging.INFO)
log = lambda msg: logging.debug(msg)
################
# Classes
Stats = namedtuple("Stats", "cpu, memory")

################
# Functions
def sys_stats(interval=1):
    """
    Return a tuple cpu and memory use eg ()
    """
    return Stats._make((psutil.cpu_percent(interval),
                        100 - psutil.virtual_memory().percent))

def run_winfates(ini_file, stutter=3):
    """
    Launch a single winfates process

    :param ini_file: path the in3 file
    :type ini_file: string
    """
    winfates_exe = r"C:\Simap\winFates.exe"
    log("Launching winfates with config file: {}".format(ini_file))
    time.sleep(randint(0, stutter))
    err = subprocess.call([winfates_exe, ini_path])
    print("Launching winfates with config file: {}".format(ini_file))

def main_seq(ns):
    """
    Launch winfates sequentially
    """
    for ini_file in ns.ini_files:
        run_winfates(ini_file, ns.stutter)
        log("sending winfates job: {}".format(ini_file))

def main_mp(ns):
    """
    Launch multiple processes
    """
    pool = multiprocessing.Pool()
    for ini_file in ns.ini_files:
        pool.apply_async(run_winfates, ini_file, ns.stutter)
        log("sending winfates job: {}".format(ini_file))
    pool.close()
    pool.join()

def main(ns):
    """
    Launch the multiprocessing or sequential programs
    """
    log("Args namespace is: \n{}".format(ns))
    ns.rundata_dir = r"stoch"
    ns.ini_files = fnmatch.filter(os.listdir(ns.rundata_dir), ns.pattern)
    if ns.debug:
        main_seq(ns)
    else:
        main_mp(ns)
################
# Setup

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("pattern", help="wildcard pattern for the in3 files")
    parser.add_argument("-j", "--jobs", default=None, type=int,
                        help="Number of parallel processes to use")
    #parser.add_argument("-m", "--mem-max", default=80, type=int,
                        #help="Max memory percentage to launch")
    #parser.add_argument("-c", "--cpu-max", default=98, type=int,
                        #help="Max cpu percentage to launch")
    parser.add_argument("-d", "--dry-run", action="store_true",
                        help="Print args but do not launch")
    parser.add_argument("--log-level", choices=["debug", "info", "warn"],
                        default="debug", help="Set logging level")
    parser.add_argument("-s", "--stutter", default=10, type=int,
                        help="Random sleep interval to sleep before launch")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--rundata-dir", default="../RUNDATA",
                        help="Directory of RUNDATA default=../RUNDATA")
    args = parser.parse_args()
    main(args)
    print("Done")
