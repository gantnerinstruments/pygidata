from datetime import datetime

project = "Pygidata (python)"
author = "Gantner Instruments"
copyright = f"{datetime.now().year}, {author}"

extensions = [
    "nbsphinx",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]

master_doc = "index"

# Optional but useful when notebooks are included
nbsphinx_execute = "never"
