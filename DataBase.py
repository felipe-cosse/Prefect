#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb  4 10:26:23 2020

@author: felipe
"""

from prefect.tasks.database import SQLiteScript

create_db = SQLiteScript(name="Create DB",
                             db="xfiles_db.sqlite",
                             script="CREATE TABLE IF NOT EXISTS XFILES (EPISODE TEXT, CHARACTER TEXT, TEXT TEXT)",
                             tags=["db"])

@task
def create_episode_script(episode):
    title, dialogue = episode
    insert_cmd = "INSERT INTO XFILES (EPISODE, CHARACTER, TEXT) VALUES\n"
    values = ',\n'.join(["('{0}', '{1}', '{2}')".format(title, *row) for row in dialogue]) + ";"
    return insert_cmd + values

insert_episode = SQLiteScript(name="Insert Episode",
                                  db="xfiles_db.sqlite",
                                  tags=["db"])

from prefect import unmapped

with flow:
    db = create_db()
    ep_script = create_episode_script.map(episode=dialogue)
    final = insert_episode.map(ep_script, upstream_tasks=[unmapped(db)])
    
flow.visualize()

state = flow.run(parameters={"url": "http://www.insidethex.co.uk/"},
                 executor=executor,
                 task_states=scraped_state.result)