#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb  4 09:17:15 2020

@author: felipe
"""

from prefect import Parameter, Flow

with Flow("Say hi!") as flow:
    name = Parameter("name")
    say_hello(name)
    
flow.run(name="Marvin")