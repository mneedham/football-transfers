import requests
from bs4 import BeautifulSoup


def find_all_pages(page):
    print(page)
    headers = {'user-agent': 'my-app/0.0.1'}
    r = requests.get(page, headers = headers)

    page = BeautifulSoup(r.content, "html.parser")

    highest_page_link = page.select("li.letzte-seite a")[0]["href"]
    highest_page_link_parts = highest_page_link.split("/")
    highest_page = int(highest_page_link_parts[-1])

    for page in range(1, highest_page + 1):
        yield page


def scrape_transfers(page):
    page = BeautifulSoup(open(page, "r"), "html.parser")

    for row in page.select("div#yw1 table.items tbody > tr"):
        columns = row.select("> td")

        if len(columns) == 5:
            player_image_element = columns[0].select("img")[0]
            player_element = columns[0].select("a.spielprofil_tooltip")[0]
            player_age = columns[1].text
            player_position = columns[0].select("table tr td")[-1].text

            from_club_elements = columns[2].select("td.hauptlink a.vereinprofil_tooltip")
            from_club_href = from_club_elements[0]["href"] if len(from_club_elements) > 0 else ""
            from_club_text = from_club_elements[0].text if len(from_club_elements) > 0 else ""

            from_club_country_image = columns[2].select("table tr td")[-1].select("img")
            from_club_country = from_club_country_image[0]["title"] if len(from_club_country_image) > 0 else ""

            to_club_elements = columns[3].select("td.hauptlink a.vereinprofil_tooltip")
            to_club_href = to_club_elements[0]["href"] if len(to_club_elements) > 0 else ""
            to_club_text = to_club_elements[0].text if len(to_club_elements) > 0 else ""

            to_club_country_image = columns[3].select("table tr td")[-1].select("img")
            to_club_country = to_club_country_image[0]["title"] if len(to_club_country_image) > 0 else ""

            fee_element = columns[4].select("a")[0]

            yield (player_element["href"],
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
                   player_image_element["src"])
