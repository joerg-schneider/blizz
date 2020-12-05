import io
import tempfile
from pathlib import Path

from dataforger._docs import serve_sphinx_html, create_sphinx_html
from dataforger._runtime import build_features, write_results
from dataforger._run_config import run_config_from_file
import click
import os
from colorama import Fore, Back, Style


@click.group()
def main():
    pass


@main.command()
@click.argument("config", type=click.Path(exists=True, resolve_path=True))
@click.argument("library_root", type=click.Path(exists=True, resolve_path=True))
@click.argument("output", type=click.Path(exists=False, resolve_path=True))
def build(library_root, output, config=None):
    """
    Build features.
    :param config:
    :return:
    """

    rc = run_config_from_file(
        file=Path(config), feature_library_base_path=Path(library_root)
    )
    click.echo(Fore.BLUE + "Feature config loaded." + Fore.RESET)
    click.echo(Fore.RED + "Building features ..." + Fore.RESET)
    r = build_features(config=rc)
    click.echo(Fore.BLUE + "Features built." + Fore.RESET)
    click.echo(Fore.RED + "Storing features ..." + Fore.RESET)
    write_results(config=rc, out_path=Path(output), results=r)
    click.echo(Fore.BLUE + "Features stored." + Fore.RESET)


@main.command()
def bootstrap():
    """
    Generate code snippets.
    :return:
    """
    click.echo(Fore.BLUE + "Initialized the database")


@main.command()
@click.argument("library_root", type=click.Path(exists=True, resolve_path=True))
@click.option("-s", "--serve", is_flag=True)
def docs(library_root, serve):
    """
    Create documentation.
    :param library_root:
    :param serve:
    :return:
    """
    if serve:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir = Path(temp_dir)
            create_sphinx_html(
                source_dir=Path(library_root), target_dir=temp_dir,
            )
            serve_sphinx_html(temp_dir.joinpath("html"))


if __name__ == "__main__":
    main()
