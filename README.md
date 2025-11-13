# Internet Game Trends Dataset 

## Overview

The **Internet Game Trends Dataset** is a curated database containing comprehensive information about video games from multiple authoritative sources. The dataset combines base game metadata from IGDB with enriched platform-specific data from Steam and Epic Games Store, creating a unified resource for game analytics targeted towards indie studios/developers.

The final curated dataset is located in `IGTDB_Snapshot_Nov_11_11/internet_game_trends.db` along with intermediate files.

## Quick Start

The easiest way to run the workflow is by using the Jupyter notebook (instructions to install jupyter are here: https://jupyter.org/install):

1. Clone the repository:
```bash
git clone https://github.com/yourusername/internet-game-trends-dataset.git
cd internet-game-trends-dataset
```

2. Launch Jupyter Notebook:
```bash
jupyter notebook
```

3. Open `project_files/workflow.ipynb` from the browser

4. Follow the instructions in the notebook to:
   - Set up your API credentials
   - Run the complete workflow
   - Generate the database
   - Explore sample queries

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
