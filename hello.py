#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb  4 09:10:53 2020

@author: felipe
"""

from prefect import task

@task
def say_hello():
    print("Hello, world!")
    
@task
def add(x, y=1):
    return x + y

@task
def say_hello(person: str) -> None:
    print("Hello, {}!".format(person))