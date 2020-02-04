#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb  4 09:12:59 2020

@author: felipe
"""

from prefect import Task

class AddTask(Task):

    def __init__(self, default: int, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.default = default

    def run(self, x: int, y: int=None) -> int:
        if y is None:
            y = self.default
        return x + y

# initialize the task instance
add = AddTask(default=1)