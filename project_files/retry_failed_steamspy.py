#!/usr/bin/env python3
"""
Retry script for failed SteamSpy fetches.

This script:
1. Loads the failed Steam IDs from failed_steamspy_fetches.json
2. Attempts to fetch SteamSpy data for each failed ID
3. Updates the corresponding entries in igdb_games_enriched.json
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
STEAMSPY_BASE_URL = 'https://steamspy.com/api'
REQUEST_DELAY = 0.25  # Delay between API requests

def load_failed_steam_ids(failed_file: str = 'failed_steamspy_fetches.json') -> List[int]:
    """Load failed Steam IDs from JSON file."""
    try:
        with open(failed_file, 'r') as f:
            failed_ids = json.load(f)
        logger.info(f"Loaded {len(failed_ids)} failed Steam IDs")
        return failed_ids
    except Exception as e:
        logger.error(f"Failed to load failed Steam IDs: {e}")
        return []

def load_enriched_games(enriched_file: str = 'igdb_games_enriched.json') -> List[Dict[str, Any]]:
    """Load enriched IGDB games data."""
    try:
        with open(enriched_file, 'r', encoding='utf-8') as f:
            games = json.load(f)
        logger.info(f"Loaded {len(games)} enriched games")
        return games
    except Exception as e:
        logger.error(f"Failed to load enriched games: {e}")
        return []

def get_steamspy_data(app_id: int) -> Optional[Dict[str, Any]]:
    """Fetch app details from SteamSpy API."""
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
            return None
            
    except Exception as e:
        logger.debug(f"Failed to fetch SteamSpy data for app ID {app_id}: {e}")
        return None

def find_steam_app_id(igdb_game: Dict[str, Any]) -> Optional[int]:
    """Find Steam app ID from IGDB game's external_games field."""
    external_games = igdb_game.get('external_games', [])
    
    for external_game in external_games:
        if external_game.get('external_game_source', {}).get('name') == 'Steam':
            try:
                return int(external_game.get('uid', 0))
            except (ValueError, TypeError):
                continue
    
    return None

def retry_failed_fetches(failed_steam_ids: List[int], enriched_games: List[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], List[int]]:
    """Retry failed SteamSpy fetches and update enriched games."""
    updated_games = []
    still_failed = []
    success_count = 0
    
    # Create a mapping of Steam app IDs to game indices for quick lookup
    steam_id_to_game_index = {}
    for i, game in enumerate(enriched_games):
        steam_app_id = find_steam_app_id(game)
        if steam_app_id:
            steam_id_to_game_index[steam_app_id] = i
    
    logger.info(f"Retrying {len(failed_steam_ids)} failed Steam IDs...")
    
    for i, steam_app_id in enumerate(failed_steam_ids):
        if i % 50 == 0:
            logger.info(f"Processed {i}/{len(failed_steam_ids)} failed IDs, {success_count} successful")
        
        # Find the corresponding game in enriched_games
        if steam_app_id in steam_id_to_game_index:
            game_index = steam_id_to_game_index[steam_app_id]
            
            # Try to fetch SteamSpy data
            steamspy_data = get_steamspy_data(steam_app_id)
            
            if steamspy_data:
                # Update the game with Steam info
                enriched_games[game_index]['steamInfo'] = steamspy_data
                success_count += 1
                logger.debug(f"Successfully fetched data for Steam ID {steam_app_id}")
            else:
                still_failed.append(steam_app_id)
            
            time.sleep(REQUEST_DELAY)
        else:
            # Steam ID not found in enriched games
            still_failed.append(steam_app_id)
    
    logger.info(f"Retry complete! {success_count}/{len(failed_steam_ids)} previously failed IDs now successful")
    logger.info(f"{len(still_failed)} IDs still failed")
    
    return enriched_games, still_failed

def save_updated_games(enriched_games: List[Dict[str, Any]], output_file: str = 'igdb_games_enriched.json'):
    """Save updated enriched games data."""
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(enriched_games, f, indent=2, ensure_ascii=False)
        logger.info(f"Updated enriched data saved to {output_file}")
    except Exception as e:
        logger.error(f"Failed to save updated data: {e}")

def save_still_failed(still_failed: List[int], output_file: str = 'failed_steamspy_fetches.json'):
    """Save still failed Steam IDs."""
    try:
        with open(output_file, 'w') as f:
            json.dump(still_failed, f)
        logger.info(f"Updated failed Steam IDs saved to {output_file}")
    except Exception as e:
        logger.error(f"Failed to save still failed IDs: {e}")

def main():
    """Main function to retry failed SteamSpy fetches."""
    print("SteamSpy Retry Tool")
    print("Retrying failed SteamSpy fetches...")
    
    # Load failed Steam IDs
    failed_steam_ids = load_failed_steam_ids()
    if not failed_steam_ids:
        logger.info("No failed Steam IDs to retry")
        return
    
    # Load enriched games
    enriched_games = load_enriched_games()
    if not enriched_games:
        logger.error("Failed to load enriched games. Exiting.")
        return
    
    # Retry failed fetches
    updated_games, still_failed = retry_failed_fetches(failed_steam_ids, enriched_games)
    
    # Save updated data
    save_updated_games(updated_games)
    save_still_failed(still_failed)
    
    print(f"\nRetry complete!")
    print(f"Originally failed: {len(failed_steam_ids)}")
    print(f"Now successful: {len(failed_steam_ids) - len(still_failed)}")
    print(f"Still failed: {len(still_failed)}")

if __name__ == '__main__':
    main()
