#!/usr/bin/env python3      
# -*- coding: utf-8 -*-

import logging


class Log:
    def __init__(self, filename):
        self.logger = logging.getLogger()
        self.logger.setLevel(level=logging.INFO)
        handler = logging.FileHandler(filename)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def info(self, message):
        self.logger.info(message)
