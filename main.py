import click
from handler import extract_transfers_clubs, extract_leagues_clubs


@click.group()
def cli():
    pass


@click.command()
@click.option('--file', default="/data/clubs.json", help='Destination file for scraped results to be written')
@click.option('--year-start', type=str, default="2018", help='start year')
@click.option('--year-end', type=str, default="2019", help='end year')
def extract_transfers_club(file, year_start, year_end):
    extract_transfers_clubs(file, int(year_start), int(year_end))


@click.command()
@click.option('--file', default="/data/clubs.json", help='Destination file for scraped results to be written')
@click.option('--year-start', type=str, default="2018", help='start year')
@click.option('--year-end', type=str, default="2019", help='end year')
def extract_leagues_club(file, year_start, year_end):
    extract_leagues_clubs(file, int(year_start), int(year_end))


cli.add_command(extract_transfers_club)
cli.add_command(extract_leagues_club)


if __name__ == '__main__':
    cli()