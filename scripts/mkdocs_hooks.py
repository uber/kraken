from pathlib import Path

from mkdocs.structure.files import File

ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_URL = "https://github.com/uber/kraken/blob/master/examples/"


def on_files(files, config):
    """Add the README and assets to the site."""
    readme = (ROOT / "README.md").read_text()
    readme = readme.replace("(docs/", "(").replace("(examples/", f"({EXAMPLES_URL}")
    files.append(File.generated(config, "index.md", content=readme))

    for asset in (ROOT / "assets").iterdir():
        files.append(File.generated(config, f"assets/{asset.name}", abs_src_path=str(asset)))
    return files
