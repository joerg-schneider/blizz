import io
import tempfile
from pathlib import Path

from dataforger._docs import serve_sphinx_html, create_sphinx_html
import click
from colorama import Fore, Back, Style


@click.group()
def main():
    pass


@main.command()
@click.argument("config", type=click.File("rt",))
def build(config: io.TextIOWrapper = None):
    print(config.read())
    click.echo("Dropped the database")


@main.command()
def bootstrap():
    click.echo(Fore.BLUE + "Initialized the database")


@main.command()
@click.argument("library_root", type=click.Path(exists=True, resolve_path=True))
@click.option("-s", "--serve", is_flag=True)
def docs(library_root, serve):
    if serve:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir = Path(temp_dir)
            create_sphinx_html(
                source_dir=Path(library_root), target_dir=temp_dir,
            )
            serve_sphinx_html(temp_dir.joinpath("html"))


@main.command()
def top():
    """ This is top."""
    click.echo("Dropped the database")


if __name__ == "__main__":
    main()
