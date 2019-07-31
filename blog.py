from datetime import date
from datetime import datetime
import click


class Date(click.ParamType):
    name = 'date'

    def __init__(self, formats=None):
        self.formats = formats or [
            '%Y-%m-%d',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%d %H:%M:%S'
        ]

    def get_metavar(self, param):
        return '[{}]'.format('|'.join(self.formats))

    def _try_to_convert_date(self, value, format):
        try:
            return datetime.strptime(value, format).date()
        except ValueError:
            return None

    def convert(self, value, param, ctx):
        for format in self.formats:
            date = self._try_to_convert_date(value, format)
            if date:
                return date

        self.fail(
            'invalid date format: {}. (choose from {})'.format(
                value, ', '.join(self.formats)))

    def __repr__(self):
        return 'Date'

@click.group()
def cli():
    pass


@click.command()
@click.option('--date-start', type=click.DateTime(formats=["%Y-%m-%d"]),
              default=str(date.today()))
@click.option('--date-end', type=click.DateTime(formats=["%Y-%m-%d"]),
              default=str(date.today()))
def dummy(date_start, date_end):
    date_start = date_start.date()
    date_end = date_end.date()
    click.echo(f"Start: {date_start}, End: {date_end} ")


@click.command()
@click.option('--date-start', type=Date(formats=["%Y-%m-%d"]), default=str(date.today()))
@click.option('--date-end', type=Date(formats=["%Y-%m-%d"]), default=str(date.today()))
def dummy2(date_start, date_end):
    click.echo(f"Start: {date_start}, End: {date_end} ")


cli.add_command(dummy)
cli.add_command(dummy2)

if __name__ == '__main__':
    cli()
