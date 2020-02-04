#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb  4 09:53:18 2020

@author: felipe
"""
from prefect import task, Flow, Parameter
from prefect.engine.executors import DaskExecutor

executor = DaskExecutor(local_processes=True)

scraped_state = flow.run(parameters={"url": "http://www.insidethex.co.uk/"},
                         executor=executor)

#    CPU times: user 9.7 s, sys: 1.67 s, total: 11.4 s
#    Wall time: 1min 34s

dialogue_state = scraped_state.result[dialogue] # list of State objects
print('\n'.join([f'{s.result[0]}: {s}' for s in dialogue_state.map_states[:5]]))