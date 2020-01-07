"""
This class handles the entire pipelines for the data collection for the following two operations:
1. extract_transfers_clubs - Extract all transfers for the clubs in a certain period.
2. extract_leagues_clubs - Extract all clubs from the given leagues in a certain period.

The pipeline looks like this:
1. Download of the required pages from transfermarkt, e.g. seasonal transfers of a club (https://www.transfermarkt.co.uk/fc-arsenal/transfers/verein/11/saison_id/2019)
2. Extracting of all the required features from this page
3. Parsing of the extracted features to json format and write to disk.

All operations are done in parallel and asynchronous to optimize the speed.
Furthermore, time is measured and displayed for the subtasks.

Written by Jan Gerling
"""

import glob
import os.path
import json
from typing import Union
import click
from joblib import Parallel, delayed
import time
from math import ceil
from multiprocessing import Process, JoinableQueue, cpu_count
from page_extractor import extract_clubs, extract_league
from Downloader import download_transfer_pages, download_league_pages, download_club_pages
from IOHelper import create_directory, move_file, parallel_write, remove_file, remove_directory
from json import JSONDecodeError
from collections import defaultdict


def parallel_jobs() -> int:
    return int(cpu_count() - 1)


def split_dic(dic, chunk_num):
    dicts = [{} for _ in range(chunk_num)]
    dict_size = ceil(len(dic) / chunk_num)
    for counter, (name, link) in enumerate(dic.items(), 0):
        dicts[int(counter / dict_size)][name] = link

    return dicts


def __extract_club_pages(file, year):
    __extract_page(file, year, __extract_club_page, "clubs")


def __extract_league_pages(file, year):
    __extract_page(file, year, __extract_league_page, "leagues")


def __extract_page(file, year, function, description):
    click.echo(f"Extracting all {description} and writing to {file}")
    q = JoinableQueue()
    p = Process(target=parallel_write, args=(q, file, ))
    p.start()
    q.put("[")
    Parallel(n_jobs=parallel_jobs(), require="sharedmem")(delayed(function)(q, file_path)
                                                          for file_path in glob.glob(f"tmp/{description}/*-{year}.html"))
    q.put("]")
    q.put(None)
    q.join()
    p.join()


def __extract_club_page(q, file_path):
    data = extract_clubs(file_path)
    __dump_page(q, file_path, data)


def __extract_league_page(q, file_path):
    data = extract_league(file_path)
    __dump_page(q, file_path, data)


def __dump_page(q, file_path, data):
    json_data = ",\n"
    json_data += json.dumps(data, indent=4)

    q.put(json_data)
    dir_path, file_name = os.path.split(file_path)
    move_file(file_path, dir_path + "/processed/" + file_name)


# Extracts all transfers with the selected features (for more Details see Report Data section) for all clubs for the given time
# and saves them to a file.
def extract_transfers_clubs(file_path, year_start, year_end):
    dir_path = os.path.dirname(__file__)
    clubs = json.load(open(dir_path + file_path, "r", encoding='utf-8'))

    __extract_entity(year_start, year_end, "clubs", clubs, download_club_pages, __extract_club_pages, "transfers")


# Extracts all clubs that were in the selected leagues for the given time and saves them to a file.
def extract_leagues_clubs(file_path, year_start, year_end):
    dir_path = os.path.dirname(__file__)
    leagues = json.load(open(dir_path + "/data/leagues_filter.json", "r+", encoding='utf-8'))

    def __extract_clubs_leagues(out_file, year):
        __extract_league_pages(out_file, year)
        __extract_clubs_from_leagues(dir_path + file_path, True)

    __extract_entity(year_start, year_end, "leagues", leagues, download_league_pages, __extract_clubs_leagues, "clubs")


def __extract_entity(year_start, year_end, descriptor, entities, downloader, extractor, additional_descriptor):
    dir_path = os.path.dirname(__file__)

    for year in range(year_start, year_end):
        create_directory(dir_path + "/tmp")
        create_directory(dir_path + f"/tmp/{descriptor}")
        create_directory(dir_path + f"/tmp/{descriptor}/processed")

        print(f"\n-------------------------------{year}-------------------------------")
        start_time = time.time()
        click.echo(f"Downloading all pages for all {len(entities)} {descriptor} for year {year}.")
        entities_dicts = split_dic(entities, parallel_jobs())
        Parallel(n_jobs=parallel_jobs())(delayed(downloader)(entities_chunk, year)
                                         for entities_chunk in entities_dicts)
        end_time = time.time()
        print(f"Downloading all {descriptor} pages took {end_time - start_time} seconds.")

        start_time = time.time()
        out_file = dir_path + f"/data/{descriptor}_{additional_descriptor}" + str(year) + ".json"
        extractor(out_file, year)
        end_time = time.time()
        print(f"Extracting data from all {descriptor} pages took {end_time - start_time} seconds.")

        remove_directory(dir_path + "/tmp")


# Extracts all clubs that were in the selected leagues for the given time and saves them to a file.
def __extract_clubs_from_leagues(path, update: bool = True):
    local_path = os.path.dirname(__file__)
    try:
        clubs_dict = json.load(open(path, "r", encoding='utf-8'))
        if not update:
            return clubs_dict
    except (FileNotFoundError, JSONDecodeError):
        clubs_dict = defaultdict(list)

    with open(path, "w+", encoding='utf-8') as clubs_file:
        for (dirpath, dirnames, filenames) in os.walk(local_path + "/data"):
            for filename in filenames:
                if "leagues_clubs" in filename:
                    data = json.load(open(dirpath + "/" + filename, "r", encoding='utf-8'))
                    for league in data:
                        try:
                            for league_key in league.keys():
                                clubs = league[league_key]
                                for club in clubs:
                                    club_name = club["club_name"]
                                    club_href = (club["club_href"]).split("saison_id")[0]
                                    if club_name not in clubs_dict:
                                        clubs_dict[club_name] = club_href
                        except AttributeError:
                            print(f"Skipping league {league} from file {filename} due to AttributeError.")
                    remove_file(dirpath + "/" + filename)
            clubs_file.seek(0)
            json.dump(clubs_dict, clubs_file, indent=4)
            print(f"Wrote a total of {len(clubs_dict.keys())} clubs into {path}.")
    return clubs_dict


# UNUSED
# This functions have been used in the past, but are now no longer used.
# They might still contain helpful functionality, thus they are not removed yet.
def collect_club_pages(clubs_path, out_file, update: bool = True):
    local_path = os.path.dirname(__file__)
    click.echo(f"Extracting all football clubs into {clubs_path}.")

    try:
        clubs_dict = json.load(open(clubs_path, "r+", encoding='utf-8'))
        if not update:
            return clubs_dict
    except FileNotFoundError:
        clubs_dict = {}

    with open(out_file, "w+") as clubs_file:
        for (dirpath, dirnames, filenames) in os.walk(local_path + "/data/transfers"):
            for filename in filenames:
                data = json.load(open(dirpath + "/" + filename, "r", encoding='utf-8'))
                for elem in data:
                    from_club = elem["from"]['name']
                    from_club_href = elem["from"]['href']
                    if from_club not in clubs_dict.keys() and len(from_club_href) > 15 and len(from_club) > 2:
                        clubs_dict[from_club] = from_club_href

                    to_club = elem["to"]['name']
                    to_club_href = elem["to"]['href']
                    if to_club not in clubs_dict.keys() and len(to_club_href) > 15 and len(to_club) > 2:
                        clubs_dict[to_club] = to_club_href

                clubs_file.seek(0)
                json.dump(clubs_dict, clubs_file, indent=4)
                click.echo(f"Wrote {len(clubs_dict)} football clubs into {out_file}.")
    return clubs_dict


def __collect_all_leagues(leagues, update: bool = True):
    local_path = os.path.dirname(__file__)
    leagues_filter_raw = local_path + "/data/leagues_filter_raw.json"
    print(f"Extracting all leagues with links for {len(leagues)} leagues into {leagues_filter_raw}.")
    leagues_dict = defaultdict(list)

    with open(leagues_filter_raw, "w+", encoding='utf-8') as leagues_file:
        for (dirpath, dirnames, filenames) in os.walk(local_path + "/data"):
            for filename in filenames:
                if "transfers_" in filename:
                    data = json.load(open(dirpath + "/" + filename, "r", encoding='utf-8'))
                    for elem in data:
                        club_name = list(elem.keys())[0]
                        club_league = elem[club_name]["club_league"]
                        if club_league in leagues \
                                and elem[club_name]["club_leagueHref"] not in leagues_dict[club_league] \
                                and club_league != "UNK" \
                                and elem[club_name]["club_leagueHref"] != "UNK":
                            leagues_dict[club_league].append(elem[club_name]["club_leagueHref"])

                    leagues_file.seek(0)
                    json.dump(leagues_dict, leagues_file, indent=4)
                    print(f"Wrote {len(leagues_dict)} football leagues into {leagues_filter_raw}.")
    return leagues_dict


def __filter_all_clubs(leagues, update: bool = True):
    local_path: Union[bytes, str] = os.path.dirname(__file__)
    clubs_path = local_path + "/data/clubs_leagues.json"
    print(f"Extracting all clubs from {len(leagues)} leagues clubs into {clubs_path}.")

    try:
        clubs_dict = json.load(open(clubs_path, "r+", encoding='utf-8'))
        if not update:
            return clubs_dict
    except (FileNotFoundError, JSONDecodeError):
        clubs_dict = defaultdict(list)

    leagues_names = {}
    for league in leagues.keys():
        leagues_names[str.split(league, "_")[0]] = league

    with open(clubs_path, "w+", encoding='utf-8') as clubs_file:
        for (dirpath, dirnames, filenames) in os.walk(local_path + "/data"):
            for filename in filenames:
                if "transfers_" in filename:
                    data = json.load(open(dirpath + "/" + filename, "r", encoding='utf-8'))
                    for elem in data:
                        club_name = list(elem.keys())[0]
                        club_league = elem[club_name]["club_league"]
                        if "UNK" not in club_league and club_league in leagues_names:
                            club_league_nat = leagues_names[club_league]

                            if club_name not in clubs_dict \
                                    and elem[club_name]["club_leagueHref"] in leagues[club_league_nat]:
                                clubs_dict[club_name] = elem[club_name]["href"]

                    clubs_file.seek(0)
                    json.dump(clubs_dict, clubs_file, indent=4)
                    print(f"Wrote {len(leagues)} football leagues with a total of {len(clubs_dict)} clubs into {clubs_path}.")
    return clubs_dict