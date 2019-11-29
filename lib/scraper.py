import requests

from bs4 import BeautifulSoup
from dateutil import parser
import soupsieve as sv
from pathvalidate import sanitize_filename

def find_all_pages(page):
    headers = {'user-agent': 'my-app/0.0.1'}
    r = requests.get(page, headers= headers)

    page = BeautifulSoup(r.content, "html.parser")
    highest_page_link_element = page.select("li.letzte-seite a")

    if len(highest_page_link_element) > 0:
        highest_page_link = highest_page_link_element[0]["href"]
        highest_page_link_parts = highest_page_link.split("/")
        highest_page = int(highest_page_link_parts[-1])

        for page in range(1, highest_page + 1):
            yield page
    else:
        yield 0


def __scrape_transfer_club(row):
    columns = sv.select(":scope > td", row)

    player_element = columns[1].find("img", {"class", "bilderrahmen-fixed"}) if not isinstance(columns[1].find("img", {"class", "bilderrahmen-fixed"}), type(None))\
        else columns[1].find("img", {"class", "bilderrahmen"})
    player_name = player_element["title"]
    player_image = player_element["src"]
    player_link = columns[1].find("td", {"class", "hauptlink"}).select("a")[0]["href"]
    player_age = columns[2].text
    player_nationality_image = columns[3].select("img")[0]["src"]
    player_nationality = columns[3].select("img")[0]["title"]
    club_name = columns[4].find("td", {"class", "hauptlink"}).find("a").text
    club_href = columns[4].find("td", {"class", "hauptlink"}).find("a")["href"]
    league = columns[4].find("td", {"class", "hauptlink"}).find("a").text
    league_href = columns[4].find("td", {"class", "hauptlink"}).find("a")["href"]
    player_fee = columns[5].select("a")[0].text
    transfer_ref = columns[5].select("a")[0]["href"]

    return {"player_name": player_name,
            "player_href": player_link,
            "player_image": player_image,
            "player_age": player_age,
            "player_nationality": player_nationality,
            "player_nationality image": player_nationality_image,
            "club_name": club_name,
            "club_href": club_href,
            "club_league": league,
            "club_league_href": league_href,
            "transfer_fee": player_fee,
            "transfer_href": transfer_ref}


def __scrape_club_season(page):
    in_row = page.findAll("a", {"class", "anchor"}, {"name", "zugaenge"})[0].parent.parent.findAll("div", {"class", "responsive-table"})[0].select("tbody > tr")
    out_row = page.findAll("a", {"class", "anchor"}, {"name", "abgaenge"})[0].parent.parent.findAll("div", {"class", "responsive-table"})[0].select("tbody > tr")
    return {"out": tuple([__scrape_transfer_club(transfer_row) for transfer_row in out_row]),
           "in": tuple([__scrape_transfer_club(transfer_row) for transfer_row in in_row])}


def __scrape_club_info(page):
    data_main = page.find("div", {"class": "dataMain"})
    club_name = data_main.find("h1", {"itemprop": "name"}).text.strip()
    image = page.find("div", {"class": "dataBild"}).select("img")[0]["src"]
    value = page.find("div", {"class": "dataMarktwert"}).select("a")[0].text.split(" ")[0]
    league_element = page.find("div", {"class": "dataZusatzDaten"})
    league_image = page.find("div", {"class": "dataZusatzImage"}).select("img")[0]["src"]

    return {"href": "UNK",
          "club_name": club_name,
          "club_image": image,
          "club_value": value,
          "club_league": league_element.find("span", {"class": "hauptpunkt"}).select("a")[0].text.strip(),
          "club_leagueHref": league_element.find("span", {"class": "hauptpunkt"}).select("a")[0]["href"],
          "club_leauge_image": league_image}


def scrape_clubs(page):
    page = BeautifulSoup(open(page, "r", encoding="utf8"), "html.parser")

    yield {"club": __scrape_club_info(page),
           "season_transfers": __scrape_club_season(page)}


def scrape_transfers2(page):
    page = BeautifulSoup(open(page, "r", encoding="utf8"), "html.parser")
    page.select("div#verein_head.row")

    date = page.select("div.box h2")[0].text.replace("Transfer on ", "")
    parsed_date = parser.parse(date)
    timestamp = parsed_date.timestamp()

    if parsed_date.month >= 7:
        season = "{start}/{end}".format(start = parsed_date.year, end = parsed_date.year+1)
    else:
        season = "{start}/{end}".format(start = parsed_date.year-1, end = parsed_date.year)

    for row in page.select("div#yw1 table.items tbody > tr"):
        columns = sv.select (":scope > td", row)

        if len(columns) == 7:
            fee_element = columns[6].select("a")[0]

            if fee_element.text == "Free Transfer":
                break

            player_image_element = columns[0].select("img")[0]
            player_element = columns[0].select("a.spielprofil_tooltip")[0]
            player_age = columns[1].text
            player_position = columns[0].select("table tr td")[-1].text

            from_club_elements = columns[3].select("td.hauptlink a.vereinprofil_tooltip")
            from_club_href = from_club_elements[0]["href"] if len(from_club_elements) > 0 else ""
            from_club_text = from_club_elements[0].text if len(from_club_elements) > 0 else ""
            from_club_image = columns[3].select("td:first-child img")[0]["src"]

            from_country_elements = columns[3].select("table tr td")[-1]
            from_club_country_image = from_country_elements.select("img")
            from_club_country = from_club_country_image[0]["title"] if len(from_club_country_image) > 0 else ""

            from_club_league_elements = from_country_elements.select("a")
            from_club_league = from_club_league_elements[0]["title"] if len(from_club_league_elements) > 0 else ""
            from_club_league_href = from_club_league_elements[0]["href"] if len(from_club_league_elements) > 0 else ""

            to_club_elements = columns[4].select("td.hauptlink a.vereinprofil_tooltip")
            to_club_href = to_club_elements[0]["href"] if len(to_club_elements) > 0 else ""
            to_club_text = to_club_elements[0].text if len(to_club_elements) > 0 else ""
            to_club_image = columns[4].select("td:first-child img")[0]["src"]

            to_country_elements = columns[4].select("table tr td")[-1]
            to_club_country_image = to_country_elements.select("img")
            to_club_country = to_club_country_image[0]["title"] if len(to_club_country_image) > 0 else ""

            to_club_league_elements = to_country_elements.select("a")
            to_club_league = to_club_league_elements[0]["title"] if len(to_club_league_elements) > 0 else ""
            to_club_league_href = to_club_league_elements[0]["href"] if len(to_club_league_elements) > 0 else ""

            yield {"season": season,
                   "player": {"href": player_element["href"],
                              "name": player_element.text,
                              "position": player_position,
                              "age": player_age,
                              "image": player_image_element["src"]},
                   "from": {"href": from_club_href,
                            "name": from_club_text,
                            "country": from_club_country,
                            "countryImage": from_club_country_image[0]["src"] if len(from_club_country_image) > 0 else "",
                            "league": from_club_league,
                            "leagueId": sanitize_filename(from_club_league_href) if from_club_league_href != "" else "",
                            "leagueHref": from_club_league_href,
                            "image": from_club_image},
                   "to": {"href": to_club_href,
                          "name": to_club_text,
                          "country": to_club_country,
                          "countryImage": to_club_country_image[0]["src"] if len(to_club_country_image) > 0 else "",
                          "league": to_club_league,
                          "leagueId": sanitize_filename(to_club_league_href) if to_club_league_href != "" else "",
                          "leagueHref": to_club_league_href,
                          "image": to_club_image},
                   "transfer": {"href": fee_element["href"],
                                "value": fee_element.text,
                                "timestamp": int(timestamp)}
                   }