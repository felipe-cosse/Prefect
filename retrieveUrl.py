#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb  4 09:34:05 2020

@author: felipe
"""

import requests
from bs4 import BeautifulSoup
from prefect import task, Flow, Parameter

@task(tags=["web"])
def retrieve_url(url):
    """
    Given a URL (string), retrieves html and
    returns the html as a string.
    """

    html = requests.get(url)
    if html.ok:
        return html.text
    else:
        raise ValueError("{} could not be retrieved.".format(url))
        
@task
def scrape_dialogue(episode_html):
    """
    Given a string of html representing an episode page,
    returns a tuple of (title, [(character, text)]) of the
    dialogue from that episode
    """

    episode = BeautifulSoup(episode_html, 'html.parser')

    title = episode.title.text.rstrip(' *').replace("'", "''")
    convos = episode.find_all('b') or episode.find_all('span', {'class': 'char'})
    dialogue = []
    for item in convos:
        who = item.text.rstrip(': ').rstrip(' *').replace("'", "''")
        what = str(item.next_sibling).rstrip(' *').replace("'", "''")
        dialogue.append((who, what))
    return (title, dialogue)

with Flow("xfiles") as flow:
    url = Parameter("url")
    episode = retrieve_url(url)
    dialogue = scrape_dialogue(episode)


flow.visualize()

episode_url = "http://www.insidethex.co.uk/transcrp/scrp320.htm"
outer_space = flow.run(parameters={"url": episode_url})

state = outer_space.result[dialogue] # the `State` object for the dialogue task
first_five_spoken_lines = state.result[1][:5] # state.result is a tuple (episode_name, [dialogue])
print(''.join([f'{speaker}: {words}' for speaker, words in first_five_spoken_lines]))