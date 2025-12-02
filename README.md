# Internet Game Trends Dataset 

## Overview

The **Internet Game Trends Dataset** is a curated database containing comprehensive information about video games from multiple authoritative sources. The dataset combines base game metadata from IGDB with enriched platform-specific data from Steam and Epic Games Store, creating a unified resource for game analytics targeted towards indie studios/developers.

The final curated dataset is located in `IGTDB_Snapshot_Nov_11_11/internet_game_trends.db` along with intermediate files.

Below is a video containing a walkthrough of the workflow:

https://www.youtube.com/watch?v=gxjK9FYtaIM

## Quick Start

The easiest way to run the workflow is by using the Jupyter notebook. This configuration may require a windows OS.

Ensure python, pip and jupyter notebook are installed:
- python: https://www.python.org/downloads/
- jupyter notebook: https://jupyter.org/install

Test that python is linked correctly. The following command should return an output in the format of `Python X.X.X`:
```
python --version
```

1. Install necessary dependencies

```
pip install -r requirements.txt
```

2. Clone the repository:
```bash
git clone https://github.com/yourusername/internet-game-trends-dataset.git
cd internet-game-trends-dataset
```

3. Launch Jupyter Notebook:
```bash
jupyter notebook
```

4. Open `project_files/workflow.ipynb` from the browser

5. Follow the instructions in the notebook to:
   - Set up your API credentials
   - Run the complete workflow
   - Generate the database
   - Explore sample queries

### Manually running the workflow

If the Jupyter notebook isn't successful, run the following scripts in order:

```
python scripts/fetch_data_from_igdb.py
python scripts/enrich_igdb_with_steam.py
python scripts/retry_failed_steamspy.py # If applicable
python scripts/enrich_igdb_with_epic_data.py
python scripts/retry_failed_steamspy.py # If applicable
python scripts/populate_igtdb.py
```

## Data Sources

| Source | Purpose | API Documentation |
|--------|---------|-------------------|
| **IGDB** | Base game metadata, ratings, genres, themes | https://api-docs.igdb.com/ |
| **Steam (via SteamSpy)** | Ownership estimates, reviews, pricing | https://steamspy.com/api.php |
| **Epic Games Store** | Pricing, age ratings, release dates | Epic GraphQL API |
| **Steam Web API** | App list for matching | https://steamapi.xpaw.me/ |

### Data Update Frequency

- The dataset represents a snapshot at the time of collection
- To get updated data, re-run the workflow scripts
- Steam app list can be refreshed from: https://steamapi.xpaw.me/#ISteamApps/GetAppList

## Provenance

This project implements full provenance tracking using the W3C PROV standard, ensuring transparency and reproducibility.

### Provenance Artifacts

- **workflow_prov.json** - Machine-readable provenance in JSON format (No retries)
- **workflow_prov.nt** - Provenance in N-Triples RDF format (No retries)
- **workflow_prov_graph.png** - Visual representation of the workflow
- **workflow_prov_graph_with_retries.png** - Visual represenation of the workflow including retry mechanisms

## Requirements

### Python Dependencies

```
duckdb>=0.9.0
prov>=2.0.0
lxml>=4.9.0
Pillow>=10.0.0
ipython>=8.0.0
jupyter>=1.0.0
python-dotenv>=1.0.0
requests>=2.31.0
```

### Hardware Specifications

Workflow has been successfully run on the following systems:

Windows

- OS: Windows 11 Pro
- Memory: 32.0 GB
- Chip: AMD Ryzen 9 7950X 16-Core Processor

## License

This dataset is released under the **CC0 1.0 Universal (CC0 1.0) Public Domain Dedication**.

You can copy, modify, distribute, and perform the work, even for commercial purposes, all without asking permission. See [LICENSE](LICENSE) for full details.

[![License: CC0-1.0](https://img.shields.io/badge/License-CC0%201.0-lightgrey.svg)](http://creativecommons.org/publicdomain/zero/1.0/)

### Data Source Licenses
- **IGDB**: https://www.igdb.com/terms
- **Steam/SteamSpy**: https://steamspy.com/about
- **Epic Games Store**: https://www.epicgames.com/site/en-US/tos

## Citation

If you use this dataset in academic research, please cite:

```bibtex
@dataset{internet_game_trends_2024,
  title={Internet Game Trends Dataset},
  author={Your Name},
  year={2024},
  url={https://github.com/yourusername/internet-game-trends-dataset},
  note={A comprehensive multi-source video game dataset}
}
```

## Metadata

```
<script type="application/ld+json">
{
  "@context": "https://schema.org/",
  "@type": "DataSet",
  "name": "Internet Game Trends Database",
  "url": "<Insert Github link w/ readme>",
  "version": "2025-12-10",
  "isAccessibleForFree": true,
  "keywords": [
    "Video Games",
    "Games",
    "IGDB",
    "Steam",
    "EPIC"
  ],
  "license": [
    "https://opendatacommons.org/licenses/odbl/1-0/"
  ],
  "citation": "J.Ma 'Internet Game Trends Database’, 2025"
}
</script>
```

