import csv
from datetime import timedelta, date, datetime
from dateutil import parser

import requests

import lib.scraper as scraper

import glob

import os.path
import json
import click
from datetime import date


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def create_directory(dir_path: str):
    if not os.path.isdir(dir_path):
        os.mkdir(dir_path)


def move_file(from_path: str, to_path:str):
    create_directory(os.path.dirname(to_path))
    os.rename(from_path, to_path)


def scrape_transfers(event, context):
    print("event: {event}".format(event=event))

    with open("/tmp/transfers.json", "w") as transfers_file:
        writer = csv.writer(transfers_file, delimiter=",")

        writer.writerow(["season",
            "playerUri", "playerName", "playerPosition", "playerAge",
            "sellerClubUri", "sellerClubName", "sellerClubCountry",
            "buyerClubUri", "buyerClubName", "buyerClubCountry",
            "transferUri", "transferFee",
            "playerImage", "playerNationality",
            "timestamp"])

        for file_path in glob.glob("data/days/*.html"):
            print(file_path)
            for row in scraper.scrape_transfers(file_path):
                writer.writerow(list(row))

def scrape_pages(event, context):
    print("event: {event}".format(event=event))

    with open("/tmp/transfers.json", "w") as transfers_file:
        writer = csv.writer(transfers_file, delimiter=",")

        # for file_path in glob.glob("data/days/*.html"):
        for file_path in glob.glob("data/days/top*.html"):
            print(file_path)
            for row in scraper.scrape_transfers2(file_path):
                print(row)
                json.dump(row,transfers_file)
                transfers_file.write("\n")


def download_pages(event, context):
    with open("/tmp/all_pages.csv", "r") as pages_file:
        reader = csv.reader(pages_file, delimiter=",")

        for row in reader:
            url = row[0]
            print(url)

            parts = url.split("/")
            single_date = parts[-5]
            page = parts[-1]

            page_path = "data/days/{single_date}-{page}.html".format(single_date=single_date, page=page)
            if not os.path.isfile(page_path):
                headers = {'user-agent': 'my-app/0.0.1'}
                response = requests.get(url, stream=True, headers=headers)
                with open(page_path, "wb") as handle:
                    for data in response.iter_content():
                        handle.write(data)


def find_all_pages(event, context):
    with open("/tmp/all_pages.csv", "a") as pages_file:
        writer = csv.writer(pages_file, delimiter=",")

        print("event: {event}".format(event=event))

        start_date = date(2019, 7, 23)
        end_date = date(2019, 7, 27)
        for single_date in daterange(start_date, end_date):
            print(single_date.strftime("%Y-%m-%d"))

            base_link_template = "https://www.transfermarkt.co.uk/transfers/transfertagedetail/statistik/top/plus/0?land_id_ab=&land_id_zu=&leihe=true&datum={single_date}"
            # base_link_template = "https://www.transfermarkt.co.uk/transfers/transfertagedetail/statistik/top/land_id_ab//land_id_zu/leihe/datum/{single_date}/plus/0/page/0"
            link_template = "https://www.transfermarkt.co.uk/transfers/transfertagedetail/statistik/top/plus/1/page/{page}?land_id_ab=&land_id_zu=&leihe=true&datum={single_date}"
            # link_template = "https://www.transfermarkt.co.uk/transfers/transfertagedetail/statistik/top/land_id_ab//land_id_zu//leihe/datum/{single_date}/plus/1/page/{page}?leihe=true"
            for page in scraper.find_all_pages(base_link_template.format(single_date=single_date)):
                writer.writerow([link_template.format(page=page, single_date=single_date)])

@click.group()
def cli():
    pass


@click.command()
@click.option('--file', default="/tmp/all_pages.csv", help='Destination file for pages to be written')
@click.option('--date-start', type = click.DateTime(formats=["%Y-%m-%d"]), default=str(date.today()), help='number of greetings')
@click.option('--date-end', type = click.DateTime(formats=["%Y-%m-%d"]), default=str(date.today()), help='number of greetings')
def find_all_pages(file, date_start, date_end):
    date_start = date_start.date()
    date_end = date_end.date()
    click.echo(f"Writing pages from {date_start} to {date_end} into {file}")
    with open(file, "a+", encoding="utf8") as pages_file:
        writer = csv.writer(pages_file, delimiter=",")
        for single_date in daterange(date_start, date_end):
            base_link_template = "https://www.transfermarkt.co.uk/transfers/transfertagedetail/statistik/top/plus/0?land_id_ab=&land_id_zu=&leihe=true&datum={single_date}"
            link_template = "https://www.transfermarkt.co.uk/transfers/transfertagedetail/statistik/top/plus/1/page/{page}?land_id_ab=&land_id_zu=&leihe=true&datum={single_date}"
            for page in scraper.find_all_pages(base_link_template.format(single_date=single_date)):
                writer.writerow([link_template.format(page=page, single_date=single_date)])


@click.command()
@click.option('--file', default="/tmp/all_pages.csv", help='Source file for pages to be downloaded')
def download_pages(file):
    click.echo(f"Downloading pages from {file}")
    with open(file, "r", encoding="utf8") as pages_file:
        reader = csv.reader(pages_file, delimiter=",")

        for row in reader:
            # filter empty lines
            if len(row) > 0 and len(row[0]) > 10:
                url = row[0]
                print(url)

                single_date = url.split("datum=")[-1]
                page = (url.split("/")[-1]).split("?")[0]

                page_path = "tmp/days/{single_date}_{page}.html".format(single_date=single_date, page=page)
                if not os.path.isfile(page_path):
                    headers = {'user-agent': 'my-app/0.0.1'}
                    response = requests.get(url, stream=True, headers=headers)
                    with open(page_path, "wb+") as handle:
                        for data in response.iter_content():
                            handle.write(data)



@click.command()
@click.option('--file', default="/tmp/transfers.json", help='Destination file for scraped results to be written')
def scrape_pages(file):
    with open(file, "w+", encoding="utf8") as transfers_file:
        for file_path in glob.glob("tmp/days/*.html"):
            for row in scraper.scrape_transfers2(file_path):
                json.dump(row, transfers_file)
                transfers_file.write("\n")
            click.echo(f"Scraped all transfers from {file_path}")

            dir_path, file_name = os.path.split(file_path)
            move_file(file_path, dir_path + "/processed/" + file_name)


cli.add_command(find_all_pages)
cli.add_command(download_pages)
cli.add_command(scrape_pages)

if __name__ == '__main__':
    cli()