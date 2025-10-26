#!/usr/bin/env python3
"""
Retry script for failed Epic Games Store fetches.

This script:
1. Loads the failed Epic fetches from failed_epic_fetches.json
2. Attempts to fetch Epic Games Store data for each failed game
3. Updates the corresponding entries in igdb_games_enriched_final.json
"""

from epicstore_api import EpicGamesStoreAPI
import json
import os
import time
from typing import List, Dict, Any, Optional

# Epic API configuration
api = EpicGamesStoreAPI()
REQUEST_DELAY = 0.05  # Delay between API requests

def load_failed_epic_fetches(failed_file: str = 'data/failed_epic_fetches.json') -> List[Dict[str, Any]]:
    """Load failed Epic fetches from JSON file."""
    try:
        with open(failed_file, 'r', encoding='utf-8') as f:
            failed_fetches = json.load(f)
        print(f"Loaded {len(failed_fetches)} failed Epic fetches")
        return failed_fetches
    except Exception as e:
        print(f"Failed to load failed Epic fetches: {e}")
        return []

def load_enriched_games(enriched_file: str = 'data/igdb_games_enriched_final.json') -> List[Dict[str, Any]]:
    """Load enriched IGDB games data."""
    try:
        with open(enriched_file, 'r', encoding='utf-8') as f:
            games = json.load(f)
        print(f"Loaded {len(games)} enriched games")
        return games
    except Exception as e:
        print(f"Failed to load enriched games: {e}")
        return []

def get_epic_data(game_name: str) -> tuple[Optional[Dict[str, Any]], bool]:
    """
    Fetch game details from Epic Games Store API using game name as keywords.
    
    Args:
        game_name (str): Name of the game to search for
    
    Returns:
        tuple: (Epic Store data or None, is_error)
            - If game found: (game_data, False)
            - If game not found: (None, False)
            - If API error: (None, True)
    """
    try:
        # Fetch store games with the game name as keywords
        response = api.fetch_store_games(count=1, keywords=game_name)
        
        if not response:
            return None, False  # Not an error, just doesn't exist
        
        # Navigate to the game elements
        if 'data' in response and 'Catalog' in response['data']:
            catalog = response['data']['Catalog']
            if 'searchStore' in catalog and 'elements' in catalog['searchStore']:
                elements = catalog['searchStore']['elements']
                
                if elements and len(elements) > 0:
                    # Return the first matching game
                    return elements[0], False
        
        return None, False  # Not an error, just doesn't exist
            
    except Exception as e:
        print(f"API error fetching Epic data for '{game_name}': {e}")
        return None, True  # This is an actual error/failed fetch

def retry_failed_fetches(failed_fetches: List[Dict[str, Any]], 
                        enriched_games: List[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Retry failed Epic fetches and update enriched games."""
    still_failed = []
    success_count = 0
    
    print(f"Retrying {len(failed_fetches)} failed Epic fetches...")
    
    for i, failed_fetch in enumerate(failed_fetches):
        if i % 10 == 0 and i > 0:
            print(f"Processed {i}/{len(failed_fetches)} failed fetches, {success_count} successful")
        
        game_name = failed_fetch.get('name', '')
        igdb_id = failed_fetch.get('igdb_id')
        
        if igdb_id is None or not game_name:
            print(f"Skipping failed fetch: missing IGDB ID or name")
            still_failed.append(failed_fetch)
            continue
        
        # Find the game by IGDB ID
        game_index = None
        for idx, game in enumerate(enriched_games):
            if game.get('id') == igdb_id:
                game_index = idx
                break
        
        if game_index is None:
            print(f"Warning: IGDB ID {igdb_id} not found for '{game_name}'")
            still_failed.append(failed_fetch)
            continue

        epic_data, is_error = get_epic_data(game_name)
        
        if epic_data:
            # Update the game with Epic info
            enriched_games[game_index]['epicInfo'] = epic_data
            success_count += 1
            print(f"Successfully fetched Epic data for '{game_name}'")
        elif is_error:
            # Still an error
            still_failed.append(failed_fetch)
        
        time.sleep(REQUEST_DELAY)
    
    print(f"\nRetry complete! {success_count}/{len(failed_fetches)} previously failed fetches now successful")
    print(f"{len(still_failed)} fetches still failed")
    
    return enriched_games, still_failed

def save_updated_games(enriched_games: List[Dict[str, Any]], 
                      output_file: str = 'data/igdb_games_enriched_final.json'):
    """Save updated enriched games data."""
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(enriched_games, f, indent=2, ensure_ascii=False)
        print(f"Updated enriched data saved to {output_file}")
    except Exception as e:
        print(f"Failed to save updated data: {e}")

def save_still_failed(still_failed: List[Dict[str, Any]], 
                     output_file: str = 'data/failed_epic_fetches.json'):
    """Save still failed Epic fetches, or delete file if empty."""
    try:
        if still_failed:
            # Save the remaining failed fetches
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(still_failed, f, indent=2, ensure_ascii=False)
            print(f"Updated failed Epic fetches saved to {output_file}")
        else:
            # No more failed fetches, delete the file if it exists
            if os.path.exists(output_file):
                os.remove(output_file)
                print(f"No more failed fetches! Deleted {output_file}")
            else:
                print("No more failed fetches!")
    except Exception as e:
        print(f"Failed to save/delete still failed fetches: {e}")

def main():
    """Main function to retry failed Epic fetches."""
    print("Epic Games Store Retry Tool")
    print("="*60)
    
    # Check if failed fetches file exists
    failed_file = 'data/failed_epic_fetches.json'
    if not os.path.exists(failed_file):
        print(f"No failed fetches file found ({failed_file})")
        print("Nothing to retry. Exiting.")
        return
    
    print("Retrying failed Epic Games Store fetches...")
    
    # Load failed Epic fetches
    failed_fetches = load_failed_epic_fetches()
    if not failed_fetches:
        print("No failed Epic fetches to retry")
        return

    # Load enriched games
    enriched_games = load_enriched_games()
    if not enriched_games:
        print("Failed to load enriched games. Exiting.")
        return

    # Retry failed fetches
    updated_games, still_failed = retry_failed_fetches(failed_fetches, enriched_games)

    # Save updated data
    save_updated_games(updated_games)
    save_still_failed(still_failed)

    print("\n" + "="*60)
    print("RETRY SUMMARY")
    print("="*60)
    print(f"Originally failed: {len(failed_fetches)}")
    print(f"Now successful: {len(failed_fetches) - len(still_failed)}")
    print(f"Still failed: {len(still_failed)}")
    print("="*60)

if __name__ == '__main__':
    main()

