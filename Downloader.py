"""
This class handles all the downloads of pages from transfermarkt.
1. Generates urls for clubs, club transfers and leagues
2. Download the pages and save the response
3. Asynchronous and parallel implementation of the two previous tasks

Written by Jan Gerling
"""

import grequests
from os import path
import requests
import re
import csv
from IOHelper import create_directory


def __save_response_hook(path):
    def __save_response(response, **kwargs):
        with open(path, "wb+") as file:
            for data in response.iter_content():
                file.write(data)
    return __save_response


def __generate_club_url(year, name, link):
    link_template = "https://www.transfermarkt.co.uk"
    return link_template + link.replace("startseite", "transfers").replace("spielplan", "transfers") + "saison_id/" + str(year), name


def __generate_club_urls(clubs, year):
    urls = [__generate_club_url(year, name, link) for name, link in clubs.items()]
    return list(filter(lambda val:
                       not path.isfile("tmp/clubs/{club}-{year}.html".format(club=re.sub('[^\w\-_. ]', '_', val[1]), year=str(year)))
                       and not path.isfile("tmp/clubs/processed/{club}-{year}.html".format(club=re.sub('[^\w\-_. ]', '_', val[1]), year=str(year))), urls))


def __generate_league_url(year, name, link):
    link_template = "https://www.transfermarkt.co.uk"
    return link_template + link.replace("startseite", "tabelle") + "?saison_id=" + str(year), name


def __generate_league_urls(leagues, year):
    urls = [__generate_league_url(year, name, link) for name, link in leagues.items()]
    return list(filter(lambda val:
                       not path.isfile("tmp/leagues/{league}-{year}.html".format(league=re.sub('[^\w\-_. ]', '_', val[1]), year=str(year)))
                       and not path.isfile("tmp/leagues/processed/{league}-{year}.html".format(league=re.sub('[^\w\-_. ]', '_', val[1]), year=str(year))), urls))


def download_club_pages(clubs, year):
    headers = {'user-agent': 'my-app/0.0.1'}
    urls = __generate_club_urls(clubs, year)
    while len(urls) > 0:
        requests_async = [grequests.get(url, headers=headers,
                                        hooks={'response': __save_response_hook("tmp/clubs/{club}-{year}.html".format(club=re.sub('[^\w\-_. ]', '_', name), year=str(year)))})
                          for url, name in urls]
        grequests.map(requests_async, size=None)
        urls = __generate_club_urls(clubs, year)


def download_league_pages(leagues, year):
    headers = {'user-agent': 'my-app/0.0.1'}
    urls = __generate_league_urls(leagues, year)
    while len(urls) > 0:
        requests_async = [grequests.get(url, headers=headers,
                                        hooks={'response': __save_response_hook("tmp/leagues/{league}-{year}.html".format(league=re.sub('[^\w\-_. ]', '_', name), year=str(year)))})
                          for url, name in urls]
        grequests.map(requests_async, size=None)
        urls = __generate_league_urls(leagues, year)


def download_transfer_pages(file):
    with open(file, "r", encoding="utf8") as pages_file:
        reader = csv.reader(pages_file, delimiter=",")
        for row in reader:
            # filter empty lines
            if len(row) > 0 and len(row[0]) > 10:
                url = row[0]

                single_date = url.split("datum=")[-1]
                page = (url.split("/")[-1]).split("?")[0]

                page_path = "tmp/days/{single_date}_{page}.html".format(single_date=single_date, page=page)
                if not path.isfile(page_path):
                    headers = {'user-agent': 'my-app/0.0.1'}
                    response = requests.get(url, stream=True, headers=headers)
                    create_directory("tmp/days")
                    with open(page_path, "wb+") as handle:
                        for data in response.iter_content():
                            handle.write(data)