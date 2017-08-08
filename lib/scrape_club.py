from bs4 import BeautifulSoup, Tag
import bs4


def is_staff_section(element):
    return element.select("span")[0].text == "Staff"


def scrape_club(event, _):
    page = BeautifulSoup(open(event['page'], "r"), "html.parser")

    league_element = page.select("div.dataZusatzbox span.hauptpunkt a")[0]
    league_name = league_element.text.strip()
    league_link = league_element["href"].strip()

    league_level_element = page.select("div.dataZusatzbox span.mediumpunkt a")[0]
    level_name = league_level_element.text.strip()

    staff_label = [element for element in page.select("div.box-slider p.text") if is_staff_section(element)][0]
    staff_container = staff_label.find_parent().find_parent()
    manager_element = staff_container.select("div.container-inhalt")[0]
    manager_link_element = manager_element.select("a")[0]
    manager_name = manager_link_element.text.strip()
    manager_link = manager_link_element["href"].strip()

    print(league_name, level_name, manager_name, manager_link)


scrape_club({"page": "data/clubs/985.html"}, "")
