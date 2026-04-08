import typer
import os
from .scraperfc import ScraperFC
from .utils.meta_scraper import MetaScraper
from rich.console import Console
from rich.table import Table
import pandas as pd

app = typer.Typer(help="ScraperFC CLI: Download football data with ease.")
console = Console()

@app.command()
def download(
    league: str = typer.Option(..., help="Name of the league (e.g. 'EPL', 'La Liga')"),
    year: int = typer.Option(..., help="Year/Season (e.g. 2023)"),
    source: str = typer.Option("fbref", help="Source: fbref, sofascore, understat"),
    stat_type: str = typer.Option("standard", help="Type of stats (standard, shooting, passing...)"),
    output: str = typer.Option(None, help="Output file path (CSV or Excel)"),
    use_cache: bool = typer.Option(True, help="Use local cache")
):
    """ Download league statistics. """
    sfc = ScraperFC(use_cache=use_cache)
    with console.status(f"[bold green]Downloading {league} {year} from {source}..."):
        try:
            df = sfc.get_league_stats(league, year, source=source, stat_type=stat_type)
            if output:
                if output.endswith('.csv'):
                    df.to_csv(output, index=False)
                elif output.endswith('.xlsx'):
                    df.to_excel(output, index=False)
                console.print(f"[bold blue]Successfully saved to {output}!")
            else:
                # Preview
                console.print(f"Preview of {league} stats:")
                console.print(df.head())
        except Exception as e:
            console.print(f"[bold red]Error: {e}")
        finally:
            sfc.close()

@app.command()
def update_leagues(
    path: str = typer.Option("comps.yaml", help="Path to comps.yaml")
):
    """ Run MetaScraper to update the list of supported leagues. """
    ms = MetaScraper(path)
    with console.status("[bold yellow]Scraping multiple sources for new leagues..."):
        ms.update_leagues()
    console.print("[bold green]Success! Base updated.")

@app.command()
def list_leagues(
    query: str = typer.Option("", help="Search for a specific league"),
    source: str = typer.Option(None, help="Filter by source")
):
    """ List all available leagues in the database. """
    sfc = ScraperFC()
    table = Table(title="Available Leagues")
    table.add_column("League Name", style="cyan")
    table.add_column("Sources", style="magenta")

    for name, data in sfc.comps.items():
        if query.lower() in name.lower():
            sources = ", ".join(data.keys())
            if not source or source.upper() in sources:
                table.add_row(name, sources)
    
    console.print(table)

@app.command()
def clear_cache():
    """ Clear all cached data. """
    from .utils.cache_manager import CacheManager
    cm = CacheManager()
    cm.clear()
    console.print("[bold red]Cache cleared!")

if __name__ == "__main__":
    app()
