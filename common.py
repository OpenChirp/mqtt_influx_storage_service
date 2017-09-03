#!/usr/bin/env python3

################################################################################
#
#  Copyright (C) 2017, Carnegie Mellon University
#  All rights reserved.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, version 2.0 of the License.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
#  Contributing Authors (specific to this file):
#
#   Artur Balanuta                  arturb[at]andrew[dot]edu
#   Khushboo Bhatia                 khush[at]cmu[dot]edu
#
################################################################################

import logging
import sys
from configparser import ConfigParser
from optparse import OptionParser


def parse_arguments():
    optp = OptionParser()
    optp.add_option('-f','--config_file', dest='config_file', help='Config file')
    opts, args = optp.parse_args()
    
    if opts.config_file is None:
        optp.print_help()
        exit()

    print("Loading configuration file: "+str(opts.config_file))

    config = ConfigParser()
    config.read(opts.config_file)

    opts = dict()

    for key in config['DEFAULT']:
        opts[key] = config['DEFAULT'][key]

    return opts

def configure_logging(conf):
    
    # create logger
    logger = logging.getLogger()
    logger.setLevel( (6 - int(conf['log_verbosity'])) * 10)
            
    # remove default logger
    while logger.handlers:
        logger.handlers.pop()

    # create file handler which logs even debug messages
    fh = logging.FileHandler(conf['log_file'])
    fh.setLevel(logging.DEBUG)
            
    # create console handler with different format
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)

    # create formatters and add it to the handlers
    f1 = logging.Formatter('[%(levelname)s’]\t - %(asctime)s - %(name)s - %(message)s')
    f2 = logging.Formatter('[%(levelname)s]\t %(message)s  - %(filename)s: %(funcName)s(): %(lineno)d:\t ')
    f2_simple = logging.Formatter('[%(levelname)s]\t [%(threadName)s]\t %(message)s')
    fh.setFormatter(f2_simple)
    ch.setFormatter(f2_simple)

    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)
