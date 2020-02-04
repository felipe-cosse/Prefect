#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb  4 09:27:42 2020

@author: felipe
"""

from prefect import task, Flow, triggers

@task
def create_cluster():
    cluster = create_spark_cluster()
    return cluster

@task
def run_spark_job(cluster):
    submit_job(cluster)

@task
def tear_down_cluster(cluster):
    tear_down(cluster)
    
@task(trigger=triggers.always_run)  # one new line
def tear_down_cluster(cluster):
    tear_down(cluster)


with Flow("Spark") as flow:
    # define data dependencies
    cluster = create_cluster()
    submitted = run_spark_job(cluster)
    tear_down_cluster(cluster)

    # wait for the job to finish before tearing down the cluster
    tear_down_cluster.set_upstream(submitted)