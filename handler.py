import csv
from datetime import timedelta, date, datetime
import requests
import lib.scraper as scraper
import glob
import os.path
import json
import click


def date_range(start_date, end_date, ):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def date_range_year(start_date, end_date):
    for n in range(int((end_date - start_date).years)):
        yield start_date + timedelta(n)


def create_directory(dir_path: str):
    if not os.path.isdir(dir_path):
        os.mkdir(dir_path)


def move_file(from_path: str, to_path:str):
    create_directory(os.path.dirname(to_path))
    os.rename(from_path, to_path)


def __find_all_pages(file, date_start, date_end):
    date_start = date_start.date()
    date_end = date_end.date()
    click.echo(f"Writing pages from {date_start} to {date_end} into {file}")
    with open(file, "w+", encoding="utf8") as pages_file:
        writer = csv.writer(pages_file, delimiter=",")
        for single_date in date_range(date_start, date_end):
            base_link_template = "https://www.transfermarkt.co.uk/transfers/transfertagedetail/statistik/top/plus/0?land_id_ab=&land_id_zu=&leihe=true&datum={single_date}"
            link_template = "https://www.transfermarkt.co.uk/transfers/transfertagedetail/statistik/top/plus/1/page/{page}?land_id_ab=&land_id_zu=&leihe=true&datum={single_date}"
            for page in scraper.find_all_pages(base_link_template.format(single_date=single_date)):
                writer.writerow([link_template.format(page=page, single_date=single_date)])


def __download_club_pages(clubs):
    click.echo(f"Downloading pages for all {len(clubs)} clubs.")
    for name, link in clubs.items():
        link_template = "https://www.transfermarkt.co.uk"
        url = link_template + link.replace("startseite", "alletransfers")
        headers = {'user-agent': 'my-app/0.0.1'}
        response = requests.get(url, stream=True, headers=headers)
        club_path = "tmp/clubs/{club}.html".format(club=name)
        with open(club_path, "wb+") as handle:
            for data in response.iter_content():
                handle.write(data)


def __download_pages(file):
    click.echo(f"Downloading pages from {file}")
    with open(file, "r", encoding="utf8") as pages_file:
        reader = csv.reader(pages_file, delimiter=",")

        for row in reader:
            # filter empty lines
            if len(row) > 0 and len(row[0]) > 10:
                url = row[0]

                single_date = url.split("datum=")[-1]
                page = (url.split("/")[-1]).split("?")[0]

                page_path = "tmp/days/{single_date}_{page}.html".format(single_date=single_date, page=page)
                if not os.path.isfile(page_path):
                    headers = {'user-agent': 'my-app/0.0.1'}
                    response = requests.get(url, stream=True, headers=headers)
                    with open(page_path, "wb+") as handle:
                        for data in response.iter_content():
                            handle.write(data)


def __scrape_pages(file):
    click.echo(f"Scraping all transfers from {file}")
    with open(file, "w+", encoding="utf8") as transfers_file:
        for file_path in glob.glob("tmp/days/*.html"):
            for row in scraper.scrape_transfers2(file_path):
                json.dump(row, transfers_file)
                transfers_file.write("\n")
            dir_path, file_name = os.path.split(file_path)
            move_file(file_path, dir_path + "/processed/" + file_name)


def __scrape_club_pages(file):
    click.echo(f"Scraping all transfers from {file}")
    with open(file, "w+", encoding="utf8") as clubs_file:
        for file_path in glob.glob("tmp/clubs/*.html"):
            for row in scraper.scrape_clubs(file_path):
                json.dump(row, clubs_file)
                clubs_file.write("\n")
            dir_path, file_name = os.path.split(file_path)
            move_file(file_path, dir_path + "/processed/" + file_name)


def __extract_transfers_ind(year_start, year_end):
    for year in range(year_start, year_end):
        print("------------------", str(year), "------------------")
        dir_path = os.path.dirname(__file__)
        pages_file = dir_path + "/tmp/transfer_pages.csv"
        date_start = datetime.strptime(str(year) + "-01-01", "%Y-%m-%d")
        date_end = datetime.strptime(str(year + 1) + "-01-01", "%Y-%m-%d")

        __find_all_pages(pages_file, date_start, date_end)
        __download_pages(pages_file)

        transfers_file = dir_path + "/data/transfers" + str(year) + ".json"
        __scrape_pages(transfers_file)


def __extract_transfers_club(year_start, year_end):
    for year in range(year_start, year_end):
        print("------------------------------------")
        dir_path = os.path.dirname(__file__)
        clubs = {'"El Ahly': '/el-ahly-kairo/startseite/verein/7',
                'Atl\u00e9tico Madrid': '/atletico-madrid/startseite/verein/13'}

        __download_club_pages(clubs)

        transfers_file = dir_path + "/data/transfers_club" + str(year) + ".json"
        __scrape_pages(transfers_file)


@click.group()
def cli():
    pass


@click.command()
@click.option('--file', default="/tmp/transfer_pages.csv", help='Destination file for pages to be written')
@click.option('--date-start', type = click.DateTime(formats=["%Y-%m-%d"]), default=str(date.today()), help='number of greetings')
@click.option('--date-end', type = click.DateTime(formats=["%Y-%m-%d"]), default=str(date.today()), help='number of greetings')
def find_all_pages(file, date_start, date_end):
    __find_all_pages(file, date_start, date_end)


@click.command()
@click.option('--file', default="/tmp/transfer_pages.csv", help='Source file for pages to be downloaded')
def download_pages(file):
    __download_pages(file)


@click.command()
@click.option('--file', default="/data/transfers.json", help='Destination file for scraped results to be written')
def scrape_pages(file):
    __scrape_pages(file)


@click.command()
@click.option('--year-start', type=str, default="2018", help='start year')
@click.option('--year-end', type=str, default="2019", help='end year')
def __extract_transfers_ind(year_start, year_end):
    __extract_transfers_ind(int(year_start), int(year_end))


@click.command()
def __extract_transfers_club():
    __extract_transfers_club()


cli.add_command(find_all_pages)
cli.add_command(download_pages)
cli.add_command(scrape_pages)
cli.add_command(__extract_transfers_ind)
cli.add_command(__extract_transfers_club)


if __name__ == '__main__':
    cli()