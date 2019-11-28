import glob
import json
from bs4 import BeautifulSoup

with open("data/leagues.json", "w") as leagues_file:
    for file_path in glob.glob("data/leagues/*"):
        page = BeautifulSoup(open(file_path, "r"), "html.parser")
        country = page.select("div.box-personeninfos table.profilheader tr td")[0].select("img")[0]["title"]
        image_elements = page.select("div.headerfoto img")
        league_name = page.select("h1.spielername-profil")[0].text
        print(file_path, country, image_elements)
        record = {
            "league": {"id": file_path.split("/")[-1].replace(".html", ""),
                       "name": league_name},
            "country": country,
            "image": image_elements[0]["src"] if len(image_elements) > 0 else ""
        }
        json.dump(record, leagues_file)
        leagues_file.write("\n")