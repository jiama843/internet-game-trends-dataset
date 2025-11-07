# Data Dictionary: Internet Game Trends Database

**Database Name:** `internet_game_trends.db`  
**Database Type:** DuckDB  

## Overview

This database contains comprehensive information about video games from multiple sources including IGDB (Internet Game Database), Steam, and Epic Games Store. The schema is designed to support analysis of game trends, ratings, genres, and platform-specific metrics.

---

## Table of Contents

1. [GameInfo](#1-gameinfo)
2. [Rating](#2-rating)
3. [Genres](#3-genres)
4. [Themes](#4-themes)
5. [GameModes](#5-gamemodes)
6. [SteamInfo](#7-steaminfo)
7. [EpicInfo](#8-epicinfo)
8. [Relationships](#relationships)

---

## 1. GameInfo

**Description:** Core table containing basic information about each game. This is the primary table that other tables reference.

**Primary Key:** `id`

| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| `id` | BIGINT | No | Unique game identifier from IGDB. Primary key. |
| `name` | VARCHAR | Yes | Official name/title of the game |
| `developer` | VARCHAR | Yes | Name of the game development company |
| `perspective` | VARCHAR | Yes | Player perspective/viewpoint (e.g. "First person", "Third person", "Bird view / Isometric") |
| `game_engine` | VARCHAR | Yes | Game engine used to develop the game (e.g. "Unreal Engine", "Unity", "MonoGame") |
| `release_date` | DATE | Yes | First official release date of the game converted from Unix timestamp |

**Example Values:**
- `id`: 300810
- `name`: "Retaliate"
- `developer`: "Romans I XVI Gaming"
- `perspective`: "Bird view / Isometric"
- `game_engine`: "MonoGame"
- `release_date`: "2024-07-15"

**Notes:**
- All fields in the GameInfo table are sourced from the IGDB API
- The `id` comes directly from IGDB's game identifier
- Each game in the dataset has a single developer
- Each game in the dataset has a single perspective
- Each game in the dataset has a single game engine
- Dates before 1970 are handled using timedelta calculations

---

## 2. Rating

**Description:** Stores various rating scores for games from different sources and rating types.

**Primary Key:** `id`   
**Foreign Key:** `game_id` → `GameInfo.id`

| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| `id` | BIGINT | No | Auto-incrementing unique identifier for each rating record. Primary key. |
| `game_id` | BIGINT | No | Reference to the game in GameInfo table. Foreign key. |
| `score` | NUMERIC | Yes | Numerical rating score |
| `score_type` | VARCHAR | Yes | Type of rating (e.g. "PERCENT_100") |
| `source` | VARCHAR | Yes | Source of the rating (e.g. "IGDB") |

**Example Values:**
- `id`: 10235
- `game_id`: 300810
- `score`: 76.0
- `score_type`: "PERCENT_100"
- `source`: "IGDB_AGGREGATED"

**Notes:**
- A single game can have multiple ratings (e.g., both aggregated and user ratings)
- Sources can have the following values:
  - `IGDB_AGGREGATED` refers to critic/professional ratings compiled by IGDB
  - `IGDB_USER` refers to player-submitted ratings compiled by IGDB
- Score type describes how the score is tracked:
  - `PERCENT_100` means the score is on a scale of 1-100

**Cardinality:** Many-to-one relationship with GameInfo (multiple ratings per game)

---

## 3. Genres

**Description:** Categorizes games by genre. A game can have multiple genres.

**Primary Key:** `id` (composite of mode ID and game ID)  
**Foreign Key:** `game_id` → `GameInfo.id`

| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| `id` | BIGINT | No | IGDB game mode identifier. Primary key (combined with game_id). |
| `game_id` | BIGINT | No | Reference to the game in GameInfo table. Foreign key. |
| `name` | VARCHAR | Yes | Name of the genre (e.g. "Shooter", "Indie", "Arcade", "RPG") |

**Example Values:**
- `id`: 10245
- `game_id`: 300810
- `name`: "Indie"

**Cardinality:** Many-to-many relationship with GameInfo

---

## 4. Themes

**Description:** Describes thematic elements of games (e.g., story setting, atmosphere). A game can have multiple themes.

**Primary Key:** `id` (composite of mode ID and game ID)  
**Foreign Key:** `game_id` → `GameInfo.id`

| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| `id` | BIGINT | No | IGDB game mode identifier. Primary key (combined with game_id). |
| `game_id` | BIGINT | No | Reference to the game in GameInfo table. Foreign key. |
| `name` | VARCHAR | Yes | Name of the theme (e.g. "Action", "Science fiction", "Fantasy", "Horror") |

**Example Values:**
- `id`: 10000
- `game_id`: 300810
- `name`: "Action"

**Cardinality:** Many-to-many relationship with GameInfo

---

## 5. GameModes

**Description:** Specifies the gameplay modes available for each game. A game can support multiple game modes.

**Primary Key:** `id` (composite of mode ID and game ID)  
**Foreign Key:** `game_id` → `GameInfo.id`

| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| `id` | BIGINT | No | IGDB game mode identifier. Primary key (combined with game_id). |
| `game_id` | BIGINT | No | Reference to the game in GameInfo table. Foreign key. |
| `name` | VARCHAR | Yes | Name of the game mode (e.g., "Single player", "Multiplayer", "Co-operative") |

**Example Values:**
- `id`: 10000
- `game_id`: 300810
- `name`: "Single player"

**Cardinality:** Many-to-many relationship with GameInfo

---

## 6. SteamInfo

**Description:** Contains Steam platform-specific information including ownership estimates, reviews, and pricing.

**Primary Key:** `id`  
**Foreign Key:** `game_id` → `GameInfo.id`

| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| `id` | BIGINT | No | Auto-incrementing unique identifier for each Steam record. Primary key. |
| `game_id` | BIGINT | No | Reference to the game in GameInfo table. Foreign key. |
| `owners_est` | INTEGER | Yes | Estimated number of game owners on Steam. Calculated as the average of the ownership range (e.g. "20,000 .. 50,000" becomes 35,000) |
| `positive_reviews` | INTEGER | Yes | Number of positive user reviews on Steam |
| `negative_reviews` | INTEGER | Yes | Number of negative user reviews on Steam |
| `initial_price_cents` | INTEGER | Yes | Original launch price in cents (USD). Example: 1999 = $19.99 |

**Example Values:**
- `id`: 10000
- `game_id`: 300810
- `owners_est`: 35000 (derived as avg from "20,000 .. 50,000")
- `positive_reviews`: 1523
- `negative_reviews`: 87
- `initial_price_cents`: 1999 (representing $19.99)

**Notes:**
- All info in the SteamInfo table is sourced from the SteamSpy API
- Not all games in GameInfo have Steam data (only those available on Steam)
- Ownership estimates are simple approximations based on Steam's public data ranges trunc(high + low / 2)
- Price is stored in cents to avoid floating-point precision issues
- Review counts refer to total number of reviews since the time of curation

**Cardinality:** One-to-one relationship with GameInfo (optional)

---

## 7. EpicInfo

**Description:** Contains Epic Games Store platform-specific information including pricing and age ratings.

**Primary Key:** `id`  
**Foreign Key:** `game_id` → `GameInfo.id`

| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| `id` | BIGINT | No | Auto-incrementing unique identifier for each Epic record. Primary key. |
| `game_id` | BIGINT | No | Reference to the game in GameInfo table. Foreign key. |
| `initial_price_cents` | INTEGER | Yes | Original launch price in cents (USD) on the Epic game store. Example: 2999 = $29.99 |
| `age_rating` | VARCHAR | Yes | Age rating/content rating (e.g. "PEGI 18", "ESRB TEEN", "ESRB MATURE") |
| `effective_date` | DATE | Yes | Date of game release on Epic Games Store |

**Example Values:**
- `id`: 10000
- `game_id`: 300810
- `initial_price_cents`: 2999 (representing $29.99)
- `age_rating`: "ESRB MATURE"
- `effective_date`: "2023-05-15"

**Notes:**
- All info in the EpicInfo table is sourced from the Epic Store API
- Not all games in GameInfo have Epic data (only those available on Epic Games Store)
- Age ratings follow various systems (ESRB, PEGI, etc.)
- Price is stored in cents to avoid floating-point precision issues

**Cardinality:** One-to-one relationship with GameInfo (optional)

---

## Relationships

### Entity Relationship Diagram (ERD) Summary

```
GameInfo (1) ──< (many) Rating
GameInfo (1) ──< (many) Genres
GameInfo (1) ──< (many) Themes
GameInfo (1) ──< (many) GameModes
GameInfo (1) ──< (many) Perspectives
GameInfo (1) ──── (0..1) SteamInfo
GameInfo (1) ──── (0..1) EpicInfo
```

---

## Data Sources

| Table | Primary Source |
|-------|---------------|
| GameInfo | IGDB API |
| Rating | IGDB API |
| Genres | IGDB API |
| Themes | IGDB API |
| GameModes | IGDB API |
| SteamInfo | Steam API via SteamSpy |
| EpicInfo | Epic Games Store GraphQL API |
