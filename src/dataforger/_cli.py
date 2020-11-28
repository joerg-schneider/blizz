import click
from colorama import Fore, Back, Style


@click.group()
def main():
    pass


@main.command()
def bootstrap():
    click.echo(Fore.BLUE + "Initialized the database")


@main.command()
def forge():
    click.echo("Dropped the database")


@main.command()
def docs():
    click.echo("Dropped the database")


@main.command()
def top():
    """ This is top."""
    click.echo("Dropped the database")


if __name__ == "__main__":
    main()
