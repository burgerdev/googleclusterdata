# -*- coding: utf-8 -*-

import os
import ConfigParser

# config is in ../conf/defaults.cfg

_this_dir = os.path.dirname(__file__)
_configfile = os.path.join(_this_dir, "..", "conf", "defaults.cfg")

config = ConfigParser.ConfigParser()
config.read(_configfile)
