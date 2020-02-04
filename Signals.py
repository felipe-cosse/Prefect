#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb  4 09:30:02 2020

@author: felipe
"""
from prefect import Flow
from prefect.engine import signals

@task
def signal_task(message):
    if message == 'go!':
        raise signals.SUCCESS(message='going!')
    elif message == 'stop!':
        raise signals.FAIL(message='stopping!')
    elif message == 'skip!':
        raise signals.SKIP(message='skipping!')

with Flow("Sgnal flow") as flow:
    signal = signal_task('go!')
    
state = flow.run()