import requests

from bs4 import BeautifulSoup
from dateutil import parser
import soupsieve as sv
from pathvalidate import sanitize_filename

def find_all_pages(page):
    print(page)
    headers = {'user-agent': 'my-app/0.0.1'}
    r = requests.get(page, headers = headers)

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


def scrape_transfers(page):
    page = BeautifulSoup(open(page, "r"), "html.parser")

    date = page.select("div.box h2")[0].text.replace("Transfer on ", "")
    parsed_date = parser.parse(date)
    timestamp = parsed_date.timestamp()

    if parsed_date.month >= 7:
        season = "{start}/{end}".format(start = parsed_date.year, end = parsed_date.year+1)
    else:
        season = "{start}/{end}".format(start = parsed_date.year-1, end = parsed_date.year)

    for row in page.select("div#yw1 table.items tbody > tr"):
        columns = row.select("> td")

        if len(columns) == 7:
            player_image_element = columns[0].select("img")[0]
            player_element = columns[0].select("a.spielprofil_tooltip")[0]
            player_age = columns[1].text
            player_position = columns[0].select("table tr td")[-1].text

            from_club_elements = columns[3].select("td.hauptlink a.vereinprofil_tooltip")
            from_club_href = from_club_elements[0]["href"] if len(from_club_elements) > 0 else ""
            from_club_text = from_club_elements[0].text if len(from_club_elements) > 0 else ""

            from_club_country_image = columns[3].select("table tr td")[-1].select("img")
            from_club_country = from_club_country_image[0]["title"] if len(from_club_country_image) > 0 else ""

            to_club_elements = columns[4].select("td.hauptlink a.vereinprofil_tooltip")
            to_club_href = to_club_elements[0]["href"] if len(to_club_elements) > 0 else ""
            to_club_text = to_club_elements[0].text if len(to_club_elements) > 0 else ""

            to_club_country_image = columns[4].select("table tr td")[-1].select("img")
            to_club_country = to_club_country_image[0]["title"] if len(to_club_country_image) > 0 else ""

            fee_element = columns[6].select("a")[0]

            nationality = columns[2].select("img")[0]["title"]

            yield (season,
                   player_element["href"],
                   player_element.text,
                   player_position,
                   player_age,
                   from_club_href,
                   from_club_text,
                   from_club_country,
                   to_club_href,
                   to_club_text,
                   to_club_country,
                   fee_element["href"],
                   fee_element.text,
                   player_image_element["src"],
                   nationality,
                   int(timestamp))


def scrape_transfers2(page):
    page = BeautifulSoup(open(page, "r", encoding="utf8"), "html.parser")

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