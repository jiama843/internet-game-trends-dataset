#!/usr/bin/env python3
"""
Script to populate DuckDB database from igdb_games_enriched_final.json
using the relational schema defined in db.schema.

This script:
1. Creates a DuckDB database with the defined schema
2. Reads the enriched JSON file
3. Populates all tables according to the relational structure
"""

import json
import duckdb
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

# Database configuration
DB_FILE = 'data/internet_game_trends.db'
JSON_FILE = 'data/igdb_games_enriched_final.json'

def create_schema(conn: duckdb.DuckDBPyConnection):
    """
    Create the database schema based on db.schema.
    
    Args:
        conn: DuckDB connection
    """
    print("Creating database schema...")

    # Create GameInfo table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS GameInfo (
            id BIGINT PRIMARY KEY,
            name VARCHAR,
            developer VARCHAR,
            perspective VARCHAR,
            game_engine VARCHAR,
            release_date DATE
        )
    """)
    
    # Create Rating table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS Rating (
            id BIGINT PRIMARY KEY,
            game_id BIGINT NOT NULL,
            score NUMERIC,
            score_type VARCHAR,
            source VARCHAR,
            FOREIGN KEY (game_id) REFERENCES GameInfo(id)
        )
    """)
    
    # Create Genres table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS Genres (
            id BIGINT PRIMARY KEY,
            game_id BIGINT NOT NULL,
            name VARCHAR,
            FOREIGN KEY (game_id) REFERENCES GameInfo(id)
        )
    """)
    
    # Create Themes table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS Themes (
            id BIGINT PRIMARY KEY,
            game_id BIGINT NOT NULL,
            name VARCHAR,
            FOREIGN KEY (game_id) REFERENCES GameInfo(id)
        )
    """)
    
    # Create GameModes table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS GameModes (
            id BIGINT PRIMARY KEY,
            game_id BIGINT NOT NULL,
            name VARCHAR,
            FOREIGN KEY (game_id) REFERENCES GameInfo(id)
        )
    """)

    # Create SteamInfo table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS SteamInfo (
            id BIGINT PRIMARY KEY,
            game_id BIGINT NOT NULL,
            owners_est INTEGER,
            positive_reviews INTEGER,
            negative_reviews INTEGER,
            initial_price_cents INTEGER,
            FOREIGN KEY (game_id) REFERENCES GameInfo(id)
        )
    """)
    
    # Create EpicInfo table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS EpicInfo (
            id BIGINT PRIMARY KEY,
            game_id BIGINT NOT NULL,
            initial_price_cents INTEGER,
            age_rating VARCHAR,
            effective_date DATE,
            FOREIGN KEY (game_id) REFERENCES GameInfo(id)
        )
    """)
    
    print("Schema created successfully")

def parse_date(date_value: Any) -> Optional[str]:
    """
    Parse various date formats to SQL date string.
    
    Args:
        date_value: Unix timestamp or date string
    
    Returns:
        Date string in YYYY-MM-DD format or None
    """
    if not date_value:
        return None
    
    try:
        # If it's a Unix timestamp
        if isinstance(date_value, (int, float)):
            # Handle negative timestamps (dates before 1970) using timedelta
            if date_value < 0:
                # For negative timestamps, calculate from epoch
                epoch = datetime(1970, 1, 1)
                dt = epoch + timedelta(seconds=date_value)
                return dt.strftime('%Y-%m-%d')
            else:
                return datetime.fromtimestamp(date_value).strftime('%Y-%m-%d')
        # If it's already a string
        elif isinstance(date_value, str):
            # Try parsing ISO format
            try:
                dt = datetime.fromisoformat(date_value.replace('Z', '+00:00'))
                return dt.strftime('%Y-%m-%d')
            except:
                return date_value
        return None
    except Exception as e:
        print(f"Could not parse date {date_value}: {e}")
        return None

def extract_owners_estimate(owners_str: str) -> Optional[int]:
    """
    Extract average from Steam owners range string (e.g., "20,000 .. 50,000").
    
    Args:
        owners_str: Owners range string
    
    Returns:
        Average of the range or None
    """
    if not owners_str or owners_str == "":
        return None
    
    try:
        # Split by ".." and clean
        parts = owners_str.split('..')
        if len(parts) == 2:
            low = int(parts[0].replace(',', '').strip())
            high = int(parts[1].replace(',', '').strip())
            return (low + high) // 2
        return None
    except Exception as e:
        print(f"Error parsing owners: {owners_str}: {e}")
        return None

def populate_game_info(conn: duckdb.DuckDBPyConnection, games: List[Dict[str, Any]]):
    """
    Populate GameInfo table.
    
    Args:
        conn: DuckDB connection
        games: List of game data
    """
    print("Populating GameInfo table...")
    
    game_data = []
    for game in games:
        # Extract perspective from player_perspectives
        perspective = None
        if 'player_perspectives' in game and game['player_perspectives']:
            perspective = game['player_perspectives'][0].get('name')
        
        # Extract developer from involved_companies, we will only have one developer due to initial filtering
        developer = None
        if 'involved_companies' in game and game['involved_companies']:
            for company in game['involved_companies']:
                if company.get('developer') == True:
                    developer = company.get('company', {}).get('name')

        # Extract game engine from game_engines
        game_engine = None
        if 'game_engines' in game and game['game_engines']:
            game_engine = game['game_engines'][0].get('name')
        
        release_date = parse_date(game.get('first_release_date'))
        
        game_data.append((
            game.get('id'),
            game.get('name'),
            developer,
            perspective,
            game_engine,
            release_date
        ))
    
    # Batch insert
    conn.executemany(
        """INSERT OR IGNORE INTO GameInfo 
           (id, name, developer, perspective, game_engine, release_date)
           VALUES (?, ?, ?, ?, ?, ?)""",
        game_data
    )
    
    print(f"Inserted {len(game_data)} games into GameInfo")

def populate_ratings(conn: duckdb.DuckDBPyConnection, games: List[Dict[str, Any]]):
    """
    Populate Rating table with aggregated and user ratings.
    
    Args:
        conn: DuckDB connection
        games: List of game data
    """
    print("Populating Rating table...")
    
    rating_data = []
    rating_id = 1
    
    for game in games:
        game_id = game.get('id')
        
        # Add aggregated rating
        if game.get('aggregated_rating'):
            rating_data.append((
                rating_id,
                game_id,
                game.get('aggregated_rating'),
                'SCALE_100',
                'IGDB_AGGREGATED'
            ))
            rating_id += 1
        
        # Add user rating
        if game.get('rating'):
            rating_data.append((
                rating_id,
                game_id,
                game.get('rating'),
                'SCALE_100',
                'IGDB_USER'
            ))
            rating_id += 1
    
    if rating_data:
        conn.executemany(
            """INSERT OR IGNORE INTO Rating 
               (id, game_id, score, score_type, source)
               VALUES (?, ?, ?, ?, ?)""",
            rating_data
        )
    
    print(f"Inserted {len(rating_data)} ratings")

def populate_genres(conn: duckdb.DuckDBPyConnection, games: List[Dict[str, Any]]):
    """
    Populate Genres table.
    
    Args:
        conn: DuckDB connection
        games: List of game data
    """
    print("Populating Genres table...")
    
    genre_data = []
    seen_genre_ids = set()
    
    for game in games:
        game_id = game.get('id')
        genres = game.get('genres', [])
        
        for genre in genres:
            genre_id = genre.get('id')
            # Use composite key of (genre_id, game_id) to avoid duplicates
            composite_key = (genre_id, game_id)
            
            if composite_key not in seen_genre_ids:
                genre_data.append((
                    genre_id,
                    game_id,
                    genre.get('name'),
                ))
                seen_genre_ids.add(composite_key)
    
    if genre_data:
        conn.executemany(
            """INSERT OR IGNORE INTO Genres 
               (id, game_id, name)
               VALUES (?, ?, ?)""",
            genre_data
        )
    
    print(f"Inserted {len(genre_data)} genre associations")

def populate_themes(conn: duckdb.DuckDBPyConnection, games: List[Dict[str, Any]]):
    """
    Populate Themes table.
    
    Args:
        conn: DuckDB connection
        games: List of game data
    """
    print("Populating Themes table...")
    
    theme_data = []
    seen_theme_ids = set()
    
    for game in games:
        game_id = game.get('id')
        themes = game.get('themes', [])
        
        for theme in themes:
            theme_id = theme.get('id')
            composite_key = (theme_id, game_id)
            
            if composite_key not in seen_theme_ids:
                theme_data.append((
                    theme_id,
                    game_id,
                    theme.get('name'),
                ))
                seen_theme_ids.add(composite_key)
    
    if theme_data:
        conn.executemany(
            """INSERT OR IGNORE INTO Themes 
               (id, game_id, name)
               VALUES (?, ?, ?)""",
            theme_data
        )
    
    print(f"Inserted {len(theme_data)} theme associations")

def populate_game_modes(conn: duckdb.DuckDBPyConnection, games: List[Dict[str, Any]]):
    """
    Populate GameModes table.
    
    Args:
        conn: DuckDB connection
        games: List of game data
    """
    print("Populating GameModes table...")
    
    mode_data = []
    seen_mode_ids = set()
    
    for game in games:
        game_id = game.get('id')
        game_modes = game.get('game_modes', [])
        
        for mode in game_modes:
            mode_id = mode.get('id')
            composite_key = (mode_id, game_id)
            
            if composite_key not in seen_mode_ids:
                mode_data.append((
                    mode_id,
                    game_id,
                    mode.get('name'),
                ))
                seen_mode_ids.add(composite_key)
    
    if mode_data:
        conn.executemany(
            """INSERT OR IGNORE INTO GameModes 
               (id, game_id, name)
               VALUES (?, ?, ?)""",
            mode_data
        )
    
    print(f"Inserted {len(mode_data)} game mode associations")

def populate_steam_info(conn: duckdb.DuckDBPyConnection, games: List[Dict[str, Any]]):
    """
    Populate SteamInfo table.
    
    Args:
        conn: DuckDB connection
        games: List of game data
    """
    print("Populating SteamInfo table...")
    
    steam_data = []
    steam_id = 1
    
    for game in games:
        if 'steamInfo' not in game:
            continue
        
        game_id = game.get('id')
        steam_info = game['steamInfo']
        
        owners_est = extract_owners_estimate(steam_info.get('owners', ''))
        
        try:
            initial_price = int(steam_info.get('initialprice', 0)) if steam_info.get('initialprice') else None
        except:
            initial_price = None
        
        steam_data.append((
            steam_id,
            game_id,
            owners_est,
            steam_info.get('positive'),
            steam_info.get('negative'),
            initial_price,
        ))
        steam_id += 1
    
    if steam_data:
        conn.executemany(
            """INSERT OR IGNORE INTO SteamInfo 
               (id, game_id, owners_est, positive_reviews, negative_reviews, 
                initial_price_cents)
               VALUES (?, ?, ?, ?, ?, ?)""",
            steam_data
        )
    
    print(f"Inserted {len(steam_data)} Steam records")

def populate_epic_info(conn: duckdb.DuckDBPyConnection, games: List[Dict[str, Any]]):
    """
    Populate EpicInfo table.
    
    Args:
        conn: DuckDB connection
        games: List of game data
    """
    print("Populating EpicInfo table...")
    
    epic_data = []
    epic_id = 1
    
    for game in games:
        if 'epicInfo' not in game:
            continue
        
        game_id = game.get('id')
        epic_info = game['epicInfo']
        
        # Extract price from Epic data
        initial_price = None
        if 'price' in epic_info and epic_info['price']:
            try:
                total_price = epic_info['price'].get('totalPrice', {})
                initial_price = total_price.get('originalPrice')
            except:
                pass
        
        # Extract age rating from categories or custom attributes
        age_rating = None
        if 'ageGatings' in epic_info and epic_info['ageGatings']:
            age_rating = epic_info['ageGatings'][0].get('ageGating')
        
        effective_date = parse_date(epic_info.get('effectiveDate'))
        
        epic_data.append((
            epic_id,
            game_id,
            initial_price,
            age_rating,
            effective_date
        ))
        epic_id += 1
    
    if epic_data:
        conn.executemany(
            """INSERT OR IGNORE INTO EpicInfo 
               (id, game_id, initial_price_cents, age_rating, effective_date)
               VALUES (?, ?, ?, ?, ?)""",
            epic_data
        )
    
    print(f"Inserted {len(epic_data)} Epic records")

def load_games_data(json_file: str) -> List[Dict[str, Any]]:
    """
    Load games data from JSON file.
    
    Args:
        json_file: Path to JSON file
    
    Returns:
        List of game dictionaries
    """
    print(f"Loading data from {json_file}...")
    
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            games = json.load(f)
        
        print(f"Loaded {len(games)} games")
        return games
    except Exception as e:
        print(f"Failed to load JSON file: {e}")
        return []

def print_summary(conn: duckdb.DuckDBPyConnection):
    """
    Print summary statistics of the populated database.
    
    Args:
        conn: DuckDB connection
    """
    print("\n" + "="*60)
    print("DATABASE POPULATION SUMMARY")
    print("="*60)
    
    tables = [
        'GameInfo', 'Rating', 'Genres', 'Themes', 
        'GameModes', 'SteamInfo', 'EpicInfo'
    ]
    
    for table in tables:
        result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        count = result[0] if result else 0
        print(f"{table:20} {count:>10,} records")
    
    # Additional statistics
    steam_games = conn.execute(
        "SELECT COUNT(*) FROM GameInfo g JOIN SteamInfo s ON g.id = s.game_id"
    ).fetchone()[0]
    
    epic_games = conn.execute(
        "SELECT COUNT(*) FROM GameInfo g JOIN EpicInfo e ON g.id = e.game_id"
    ).fetchone()[0]
    
    print("\n" + "-"*60)
    print(f"Games with Steam data:     {steam_games:>10,}")
    print(f"Games with Epic data:      {epic_games:>10,}")
    print("="*60)

def main():
    """Main execution function."""
    print("IGDB Games Database Population Tool")
    print("="*60)
    
    # Delete existing database file if it exists
    if os.path.exists(DB_FILE):
        print(f"Deleting existing database file: {DB_FILE}")
        os.remove(DB_FILE)
        print("Existing database deleted.")
    
    # Load games data
    games = load_games_data(JSON_FILE)
    if not games:
        print("No games data loaded. Exiting.")
        return
    
    # Connect to DuckDB
    print(f"Connecting to DuckDB database: {DB_FILE}")
    conn = duckdb.connect(DB_FILE)
    
    try:
        # Create schema
        create_schema(conn)
        
        # Populate tables
        populate_game_info(conn, games)
        populate_ratings(conn, games)
        populate_genres(conn, games)
        populate_themes(conn, games)
        populate_game_modes(conn, games)
        populate_steam_info(conn, games)
        populate_epic_info(conn, games)
        
        # Commit changes
        conn.commit()
        
        # Print summary
        print_summary(conn)
        
        print("Database population completed successfully!")
        
    except Exception as e:
        print(f"Error populating database: {e}")
        conn.rollback()
        raise
    
    finally:
        conn.close()
        print("Database connection closed")

if __name__ == "__main__":
    main()

