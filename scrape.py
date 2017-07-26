from bs4 import BeautifulSoup, Tag

import bs4

page = BeautifulSoup(open("data/days/2017-07-24.html", "r"), "html.parser")

for row in page.select("div#yw1 table.items tbody tr"):
    columns = row.select("td")

    if len(columns) >= 5:
        player_element = columns[0].select("a.spielprofil_tooltip")[0]
        from_club_element = columns[5].select("td.hauptlink a.vereinprofil_tooltip")[0]
        to_club_element = columns[9].select("td.hauptlink a.vereinprofil_tooltip")[0]
        fee_element = columns[13].select("a")[0]

        print(player_element["href"], player_element.text, \
              from_club_element["href"], from_club_element.text, \
              to_club_element["href"], to_club_element.text, \
              fee_element["href"], fee_element.text)
