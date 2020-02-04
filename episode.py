#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb  4 09:41:31 2020

@author: felipe
"""

import requests
from bs4 import BeautifulSoup
from prefect import task, Flow, Parameter

@task
def create_episode_list(base_url, main_html, bypass):
    """
    Given the main page html, creates a list of episode URLs
    """

    if bypass:
        return [base_url]

    main_page = BeautifulSoup(main_html, 'html.parser')

    episodes = []
    for link in main_page.find_all('a'):
        url = link.get('href')
        if 'transcrp/scrp' in (url or ''):
            episodes.append(base_url + url)

    return episodes

  
    
with Flow("xfiles") as flow:
    url = Parameter("url")
    bypass = Parameter("bypass", default=False, required=False)
    home_page = retrieve_url(url)
    episodes = create_episode_list(url, home_page, bypass=bypass)
    episode = retrieve_url.map(episodes)
    dialogue = scrape_dialogue.map(episode)
    
flow.visualize()


scraped_state = flow.run(parameters={"url": "http://www.insidethex.co.uk/"})
#    CPU times: user 7.48 s, sys: 241 ms, total: 7.73 s
#    Wall time: 4min 46s

dialogue_state = scraped_state.result[dialogue] # list of State objects
print('\n'.join([f'{s.result[0]}: {s}' for s in dialogue_state.map_states[:5]]))