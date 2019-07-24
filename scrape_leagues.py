import glob
import json

from bs4 import BeautifulSoup

with open("data/leagues.json", "w") as leagues_file:
    for file_path in glob.glob("data/leagues/*"):
        page = BeautifulSoup(open(file_path, "r"), "html.parser")
        country = page.select("div.box-personeninfos table.profilheader tr td")[0].select("img")[0]["title"]
        print(file_path, country)
        record = {
            "league": file_path.split("/")[-1].replace(".html", ""),
            "country": country
        }
        json.dump(record, leagues_file)
        leagues_file.write("\n")