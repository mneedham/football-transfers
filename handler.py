import csv
from datetime import timedelta, date

import requests

import lib.scraper as scraper

import glob

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def scrape_transfers(event, context):
    print("event: {event}".format(event=event))

    with open("/tmp/transfers.csv", "w") as transfers_file:
        writer = csv.writer(transfers_file, delimiter=",")
        writer.writerow(["season",
            "playerUri", "playerName", "playerPosition", "playerAge",
            "sellerClubUri", "sellerClubName", "sellerClubCountry",
            "buyerClubUri", "buyerClubName", "buyerClubCountry",
            "transferUri", "transferFee",
            "playerImage"])

        for file_path in glob.glob("data/days/*.html"):
            print(file_path)
            for row in scraper.scrape_transfers(file_path):
                print(list(row))
                writer.writerow(["16/17"] + list(row))


def download_pages(event, context):
    with open("/tmp/pages.csv", "r") as pages_file:
        reader = csv.reader(pages_file, delimiter=",")

        for row in reader:
            url = row[0]
            print(url)

            parts = url.split("/")
            single_date = parts[-5]
            page = parts[-1]

            headers = {'user-agent': 'my-app/0.0.1'}
            response = requests.get(url, stream=True, headers=headers)
            with open("data/days/{single_date}-{page}.html".format(single_date=single_date, page=page), "wb") as handle:
                for data in response.iter_content():
                    handle.write(data)


def find_all_pages(event, context):
    with open("/tmp/pages.csv", "w") as pages_file:
        writer = csv.writer(pages_file, delimiter=",")

        print("event: {event}".format(event=event))

        start_date = date(2017, 8, 1)
        end_date = date(2017, 8, 4)
        for single_date in daterange(start_date, end_date):
            print(single_date.strftime("%Y-%m-%d"))

            link_template = "https://www.transfermarkt.co.uk/transfers/transfertagedetail/statistik/top/land_id_ab//land_id_zu//leihe//datum/{single_date}/plus/0/page/{page}"
            for page in scraper.find_all_pages(link_template.format(single_date=single_date, page=0)):
                writer.writerow([link_template.format(page=page, single_date=single_date)])
