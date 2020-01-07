"""
This class extracts all the data from pages from transfermarkt and parses them to json format.
1. Extract transfers from a clubs transfer page for a specific season, will all required features, see report for more Details.
2. Extract club name and link from a league page for a specific season
3. Error handling and missing data/ feature points handling

Written by Jan Gerling based on https://github.com/mneedham/football-transfers/blob/master/scrape_leagues.py
"""

import logging
import traceback
from bs4 import BeautifulSoup
import soupsieve as sv


def __extract_transfer_club(row, transfer_direction):
    columns = sv.select(":scope > td", row)
    if len(columns) != 6:
        return {"None"}

    player_element = columns[1].find("img", {"class", "bilderrahmen-fixed"}) if not isinstance(columns[1].find("img", {"class", "bilderrahmen-fixed"}), type(None))\
        else columns[1].find("img", {"class", "bilderrahmen"})
    player_name = player_element["title"]
    player_image = player_element["src"]
    player_link = columns[1].find("td", {"class", "hauptlink"}).select("a")[0]["href"]
    player_link = player_link if len(player_link) > 0 else "UNK"
    player_age = columns[2].text
    try:
        player_nationality = ','.join([columns[3].select("img")[counter]["title"] for counter in range(len(columns[3].select("img")))])
        player_nationality_image = ', '.join([columns[3].select("img")[counter]["src"] for counter in range(len(columns[3].select("img")))])
    except IndexError:
        print(f"Error occurred while retrieving player nationality or nationality image for player {player_name}")
        player_nationality = "UNK"
        player_nationality_image = "UNK"

    club_name = columns[4].find("td", {"class", "hauptlink"}).find("a").text
    club_href = columns[4].find("td", {"class", "hauptlink"}).find("a")["href"]
    try:
        league_element = columns[4].find("img", {"class", "flaggenrahmen"}).parent
        league = league_element.find("a")["title"].split('(')[0]
        league_href = league_element.find("a")["href"]
    except (TypeError, AttributeError):
        league = 'UNK'
        league_href = 'UNK'

    player_fee = columns[5].select("a")[0].text
    transfer_ref = columns[5].select("a")[0]["href"]

    return {"player_name": player_name,
            "player_href": player_link,
            "player_image": player_image,
            "player_age": player_age,
            "player_nationality": player_nationality,
            "player_nationality image": player_nationality_image,
            f"{transfer_direction}_club_name": club_name,
            f"{transfer_direction}_club_href": club_href,
            f"{transfer_direction}_club_league": league,
            f"{transfer_direction}_club_league_href": league_href,
            "transfer_fee": player_fee,
            "transfer_href": transfer_ref}


def __extract_club_season(page):
    try:
        arrival_box = page.findAll("a", {"class", "anchor"}, {"name", "zugaenge"})[0].parent.parent
        from_row = arrival_box.select("div:is(.responsive-table)")[0].select("tbody > tr")
    except IndexError:
        from_row = []
    try:
        departure_box = page.findAll("a", {"class", "anchor"}, {"name", "abgaenge"})[1].parent.parent
        to_row = departure_box.select("div:is(.responsive-table)")[0].select("tbody > tr")
    except IndexError:
        to_row = []

    return {"Arrivals": tuple([__extract_transfer_club(transfer_row, "from") for transfer_row in from_row]),
            "Departures": tuple([__extract_transfer_club(transfer_row, "to") for transfer_row in to_row])}


def __extract_club_name(page):
    return page.find("h1", {"itemprop": "name"}).text.strip()


def __extract_league_name(page):
    try:
        name = page.find("div", {"class": "header-foto"}).select("img")[0]["title"]
    except AttributeError:
        try:
            name = page.find("div", {"class": "headerfoto"}).select("img")[0]["title"]
        except AttributeError:
            name = page.find("h1", {"class": "spielername-profil"}).text
    return name


def __extract_club_info(page,):
    club_name = __extract_club_name(page)
    club_href = page.find("a", {"class", "megamenu"}, {"name", "SubNavi"})["href"]
    image = page.find("div", {"class": "dataBild"}).select("img")[0]["src"] \
        if not isinstance(page.find("div", {"class": "dataBild"}), type(None)) else "UNK"
    value = page.find("div", {"class": "dataMarktwert"}).select("a")[0].text.split(" ")[0] \
        if not isinstance(page.find("div", {"class": "dataMarktwert"}), type(None)) else "UNK"
    league_element = page.find("div", {"class": "dataZusatzDaten"}) \
        if not isinstance(page.find("div", {"class": "dataZusatzDaten"}), type(None)) else "UNK"
    club_league = league_element if league_element == "UNK" else league_element.find("span", {"class": "hauptpunkt"}).select("a")[0].text.strip()
    club_league_href = league_element if league_element == "UNK" else league_element.find("span", {"class": "hauptpunkt"}).select("a")[0]["href"]
    try:
        league_image = page.find("div", {"class": "dataZusatzImage"}).select("img")[0]["src"]
    except (IndexError, AttributeError):
        league_image = "UNK"

    return {"href": club_href,
            "club_name": club_name,
            "club_image": image,
            "club_value": value,
            "club_league": club_league,
            "club_leagueHref": club_league_href,
            "club_league_image": league_image,
            "season_transfers": __extract_club_season(page)}\


def __extract_league_club_info(row):
    columns = sv.select(":scope > td", row)
    club_name_element = columns[2].findAll("a")[0]
    name = club_name_element.text
    href = club_name_element["href"]
    return {"club_name": str(name),
            "club_href": str(href)}


def __extract_league_info(page,):
    rows = page.findAll("div", {"class", "large-8 columns"})[0].parent.parent.findAll("div", {"class", "responsive-table"})[0].select("tbody > tr")
    clubs = tuple([__extract_league_club_info(row) for row in rows])
    return clubs


def extract_clubs(page_path):
    page = BeautifulSoup(open(page_path, "r", encoding="utf8"), "html.parser")
    try:
        return {str(__extract_club_name(page)): __extract_club_info(page)}
    except (IndexError, AttributeError) as _:
        try:
            print(f"Error occurred while scraping club {__extract_club_name(page)}")
        finally:
            print(f"Failed to parse club name from page {page_path}")
            logging.error(traceback.format_exc())
            return ""


def extract_league(page_path):
    page = BeautifulSoup(open(page_path, "r", encoding="utf8"), "html.parser")
    try:
        return {str(__extract_league_name(page)): __extract_league_info(page)}
    except (IndexError, AttributeError) as _:
        try:
            print(f"Error occurred while scraping league {__extract_league_name(page)}")
        finally:
            print(f"Failed to parse league name from page {page_path}")
            logging.error(traceback.format_exc())
            return ""