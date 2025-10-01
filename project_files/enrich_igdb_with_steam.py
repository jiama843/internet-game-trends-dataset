#!/usr/bin/env python3
"""
Script to enrich IGDB games data with Steam information from SteamSpy API.

For each IGDB game that has a corresponding Steam app ID, this script:
1. Finds the Steam app ID from the external_games field
2. Makes a call to SteamSpy API to get additional app details
3. Adds a 'steamInfo' field to the IGDB game data with the SteamSpy response
"""

import json
import logging
import requests
import time
from typing import List, Dict, Any, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# SteamSpy API configuration
STEAMSPY_BASE_URL = 'https://steamspy.com/api.php'
REQUEST_DELAY = 0.25  # Delay between API requests to be respectful

def load_igdb_games(igdb_games_file: str = 'igdb_games.json') -> List[Dict[str, Any]]:
    """
    Load IGDB games data.
    
    Args:
        igdb_games_file (str): Path to IGDB games JSON file
    
    Returns:
        List[Dict]: List of IGDB games
    """
    logger.info("Loading IGDB games data...")
    try:
        with open(igdb_games_file, 'r', encoding='utf-8') as f:
            igdb_games = json.load(f)
        
        logger.info(f"Loaded {len(igdb_games)} IGDB games")
        return igdb_games
        
    except Exception as e:
        logger.error(f"Failed to load IGDB games: {e}")
        return []

def load_steam_apps(steam_applist_file: str = 'steam_applist.json') -> Dict[int, Dict[str, Any]]:
    """
    Load Steam app list and create a dictionary mapping app IDs to app data.
    
    Args:
        steam_applist_file (str): Path to Steam app list JSON file
    
    Returns:
        Dict[int, Dict]: Dictionary mapping Steam app IDs to app data
    """
    logger.info("Loading Steam app list...")
    try:
        with open(steam_applist_file, 'r', encoding='utf-8') as f:
            steam_data = json.load(f)
        
        steam_apps = steam_data['applist']['apps']
        steam_apps_dict = {app['appid']: app for app in steam_apps}
        
        logger.info(f"Loaded {len(steam_apps_dict)} Steam apps")
        return steam_apps_dict
        
    except Exception as e:
        logger.error(f"Failed to load Steam app list: {e}")
        return {}

def get_steamspy_data(app_id: int) -> Optional[Dict[str, Any]]:
    """
    Fetch app details from SteamSpy API.
    
    Args:
        app_id (int): Steam app ID
    
    Returns:
        Optional[Dict]: SteamSpy data or None if request fails
    """
    try:
        params = {
            'request': 'appdetails',
            'appid': app_id
        }
        
        response = requests.get(STEAMSPY_BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()

        if data and data.get('appid') == app_id:
            return data
        else:
            logger.debug(f"No valid data returned for app ID {app_id}")
            return None
            
    except requests.exceptions.RequestException as e:
        logger.warning(f"Failed to fetch SteamSpy data for app ID {app_id}: {e}")
        return None
    except json.JSONDecodeError as e:
        logger.warning(f"Invalid JSON response for app ID {app_id}: {e}")
        return None
    except Exception as e:
        logger.warning(f"Unexpected error fetching data for app ID {app_id}: {e}")
        return None

def find_steam_app_id(igdb_game: Dict[str, Any]) -> Optional[int]:
    """
    Find Steam app ID from IGDB game's external_games field.
    
    Args:
        igdb_game (Dict): IGDB game data
    
    Returns:
        Optional[int]: Steam app ID or None if not found
    """
    external_games = igdb_game.get('external_games', [])
    steam_category_id = 1  # Steam's category ID in IGDB external_games
    
    for external_game in external_games:
        if external_game.get('external_game_source')['name'] == 'Steam':
            try:
                return int(external_game.get('uid', 0))
            except (ValueError, TypeError):
                continue
    
    return None

def enrich_igdb_with_steam_data(igdb_games: List[Dict[str, Any]], 
                               steam_apps_dict: Dict[int, Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Enrich IGDB games with Steam information from SteamSpy API.
    
    Args:
        igdb_games (List[Dict]): List of IGDB games
        steam_apps_dict (Dict): Dictionary of Steam apps
    
    Returns:
        List[Dict]: Enriched IGDB games with steamInfo field where applicable
    """
    enriched_games = []
    failed_steam_ids = []
    steam_enriched_count = 0
    games_to_process = igdb_games
    
    logger.info(f"Processing {len(games_to_process)} IGDB games...")
    
    for i, igdb_game in enumerate(games_to_process):
        if i % 500 == 0:
            logger.info(f"Processed {i}/{len(games_to_process)} games, enriched {steam_enriched_count} with Steam data")
        
        enriched_game = igdb_game.copy()
        steam_app_id = find_steam_app_id(igdb_game)
        
        if steam_app_id and steam_app_id in steam_apps_dict:
            steamspy_data = get_steamspy_data(steam_app_id)
            
            if steamspy_data:
                enriched_game['steamInfo'] = steamspy_data
                steam_enriched_count += 1
                logger.debug(f"Enriched '{igdb_game.get('name', 'Unknown')}' with Steam data (App ID: {steam_app_id})")
            else:
                failed_steam_ids.append(steam_app_id)

            time.sleep(REQUEST_DELAY)
        
        enriched_games.append(enriched_game)
    
    logger.info(f"Enrichment complete! {steam_enriched_count}/{len(games_to_process)} games enriched with Steam data")

    if failed_steam_ids:
        with open('failed_steamspy_fetches.json', 'w') as f:
            json.dump(failed_steam_ids, f)
    
    return enriched_games

def save_enriched_data(enriched_games: List[Dict[str, Any]], 
                      output_file: str = 'igdb_games_enriched.json'):
    """
    Save enriched IGDB games data to JSON file.
    
    Args:
        enriched_games (List[Dict]): Enriched games data
        output_file (str): Output file path
    """
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(enriched_games, f, indent=2, ensure_ascii=False)
        logger.info(f"Enriched data saved to {output_file}")
    except Exception as e:
        logger.error(f"Failed to save enriched data: {e}")

def print_enrichment_summary(enriched_games: List[Dict[str, Any]]):
    """
    Print a summary of the enrichment process.
    
    Args:
        enriched_games (List[Dict]): Enriched games data
    """
    total_games = len(enriched_games)
    games_with_steam_info = sum(1 for game in enriched_games if 'steamInfo' in game)
    
    print("\n" + "="*60)
    print("IGDB-STEAM ENRICHMENT SUMMARY")
    print("="*60)
    print(f"Total IGDB games processed: {total_games:,}")
    print(f"Games enriched with Steam data: {games_with_steam_info:,}")
    print(f"Enrichment rate: {(games_with_steam_info/total_games*100):.1f}%")
    
    if games_with_steam_info > 0:
        print(f"\nSample of enriched games:")
        count = 0
        for game in enriched_games:
            if 'steamInfo' in game and count < 5:
                steam_info = game['steamInfo']
                igdb_name = game.get('name', 'Unknown')
                steam_name = steam_info.get('name', 'Unknown')
                owners = steam_info.get('owners', 'Unknown')
                print(f"{count+1}. IGDB: '{igdb_name}' -> Steam: '{steam_name}' (Owners: {owners})")
                count += 1
    
    print("="*60)

def main():
    """Main function to orchestrate the enrichment process."""
    print("IGDB-Steam Enrichment Tool")
    print("Enriching IGDB games with Steam data from SteamSpy API...")
    
    # Load data
    igdb_games = load_igdb_games()
    if not igdb_games:
        logger.error("Failed to load IGDB games. Exiting.")
        return
    
    steam_apps_dict = load_steam_apps()
    if not steam_apps_dict:
        logger.error("Failed to load Steam apps. Exiting.")
        return
    
    has_external_games = any('external_games' in game for game in igdb_games[:10])
    if not has_external_games:
        logger.error("IGDB data does not contain external_games field. Cannot find Steam app IDs.")
        return
    
    print(f"\nFound {len(igdb_games):,} IGDB games to process.")
    print("Note: This will make API calls to SteamSpy, which may take a while.")
    
    enriched_games = enrich_igdb_with_steam_data(igdb_games, steam_apps_dict)
    
    if enriched_games:
        save_enriched_data(enriched_games)
        print_enrichment_summary(enriched_games)
        
    else:
        logger.error("No games were processed successfully.")

if __name__ == '__main__':
    main()
