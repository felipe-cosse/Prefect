#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb  4 09:26:28 2020

@author: felipe
"""
from prefect import Parameter, Flow

flow = Flow("My imperative flow!")

# define some new tasks
name = Parameter("name")
second_add = add.copy()

# add our tasks to the flow
flow.add_task(add)
flow.add_task(second_add)
flow.add_task(say_hello)

# create non-data dependencies so that `say_hello` waits for `second_add` to finish.
say_hello.set_upstream(second_add, flow=flow)

# create data bindings
add.bind(x=1, y=2, flow=flow)
second_add.bind(x=add, y=100, flow=flow)
say_hello.bind(person=name, flow=flow)