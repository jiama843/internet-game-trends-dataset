#!/usr/bin/env python3
"""
Script to enrich IGDB games data with Epic Games Store information.

For each IGDB game, this script:
1. Uses the game name as search keywords in the Epic Games Store API
2. Fetches the first matching game from Epic Store
3. Adds an 'epicInfo' field to the IGDB game data with the Epic Store response

Note: Failed fetches are only recorded for actual API errors (network issues, timeouts, etc.)
Games that simply don't exist on Epic Store are not considered failures.
"""

from epicstore_api import EpicGamesStoreAPI, OfferData
import json
import time
import os
from typing import List, Dict, Any, Optional

# Epic API configuration
api = EpicGamesStoreAPI()
REQUEST_DELAY = 0.05  # Delay between API requests to be respectful
BATCH_SIZE = 500  # Number of games to process per batch (saves after each batch)

def load_igdb_games(igdb_games_file: str = 'data/igdb_games_enriched_steam.json') -> List[Dict[str, Any]]:
    """
    Load IGDB games data from the enriched file.
    
    Args:
        igdb_games_file (str): Path to IGDB games JSON file
    
    Returns:
        List[Dict]: List of IGDB games
    """
    print("Loading IGDB games data...")
    
    try:
        with open(igdb_games_file, 'r', encoding='utf-8') as f:
            igdb_games = json.load(f)
        
        print(f"Loaded {len(igdb_games)} IGDB games from {igdb_games_file}")
        return igdb_games
        
    except Exception as e:
        print(f"Failed to load IGDB games from {igdb_games_file}: {e}")
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
            # print(f"No response for game '{game_name}' - game doesn't exist on Epic")
            return None, False  # Not an error, just doesn't exist
        
        # Navigate to the game elements
        if 'data' in response and 'Catalog' in response['data']:
            catalog = response['data']['Catalog']
            if 'searchStore' in catalog and 'elements' in catalog['searchStore']:
                elements = catalog['searchStore']['elements']
                
                if elements and len(elements) > 0:
                    # Return the first matching game
                    return elements[0], False
        
        # print(f"No matching game found on Epic for '{game_name}'")
        return None, False  # Not an error, just doesn't exist
            
    except Exception as e:
        print(f"API error fetching Epic data for '{game_name}': {e}")
        return None, True  # This is an actual error/failed fetch

def process_batch(batch_games: List[tuple[int, Dict[str, Any]]], 
                 enriched_games: List[Dict[str, Any]],
                 failed_fetches: List[Dict[str, Any]]) -> int:
    """
    Process a batch of games, making API calls for each.
    
    Args:
        batch_games (List[tuple]): List of (index, game) tuples to process
        enriched_games (List[Dict]): Full list of enriched games (modified in place)
        failed_fetches (List[Dict]): List of failed fetches (modified in place)
    
    Returns:
        int: Number of games enriched in this batch
    """
    enriched_count = 0
    
    for i, igdb_game in batch_games:
        game_name = igdb_game.get('name', '')
        
        # Skip if already has epicInfo
        if 'epicInfo' in igdb_game:
            # print(f"Skipping '{game_name}' - already has epicInfo")
            enriched_count += 1
            continue
        
        if not game_name:
            # print(f"Skipping game at index {i} - no name found")
            continue
        
        # Fetch Epic data
        epic_data, is_error = get_epic_data(game_name)
        
        if epic_data:
            enriched_games[i]['epicInfo'] = epic_data
            enriched_count += 1
            # print(f"Enriched '{game_name}' with Epic data")
        elif is_error:
            failed_fetches.append({
                'igdb_id': igdb_game.get('id'),
                'name': game_name,
                'index': i,
                'reason': 'API error'
            })
            print(f"Failed to fetch '{game_name}' - API error")
        
        # Delay to prevent rate limiting
        time.sleep(REQUEST_DELAY)
    
    return enriched_count

def enrich_igdb_with_epic_data(igdb_games: List[Dict[str, Any]], 
                                start_index: int = 0) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Enrich IGDB games with Epic Games Store information in batches.
    
    Args:
        igdb_games (List[Dict]): List of IGDB games
        start_index (int): Index to start processing from (for resuming)
    
    Returns:
        tuple: (enriched_games, failed_fetches)
    """
    enriched_games = igdb_games.copy()
    failed_fetches = []
    epic_enriched_count = 0
    
    print(f"Processing {len(igdb_games)} IGDB games in batches of {BATCH_SIZE} (starting from index {start_index})...")
    
    # Process games in batches
    for batch_start in range(start_index, len(enriched_games), BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, len(enriched_games))
        
        print(f"Processing batch {batch_start}-{batch_end} ({batch_end}/{len(enriched_games)} total)")
        
        # Collect games for this batch
        batch_games = [(i, enriched_games[i]) for i in range(batch_start, batch_end)]
        
        # Process the batch
        batch_enriched = process_batch(batch_games, enriched_games, failed_fetches)
        epic_enriched_count += batch_enriched
        
        print(f"Batch complete: enriched {batch_enriched}/{len(batch_games)} games. Total: {epic_enriched_count}")
        
        # Save progress after each batch
        save_enriched_data(enriched_games, output_file='data/igdb_games_enriched_final.json')
    
    print(f"Enrichment complete! {epic_enriched_count}/{len(enriched_games)} games enriched with Epic data")
    
    return enriched_games, failed_fetches

def save_enriched_data(enriched_games: List[Dict[str, Any]], 
                      output_file: str = 'igdb_games_enriched_final.json'):
    """
    Save enriched IGDB games data to JSON file.
    
    Args:
        enriched_games (List[Dict]): Enriched games data
        output_file (str): Output file path
    """
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(enriched_games, f, indent=2, ensure_ascii=False)
        print(f"Enriched data saved to {output_file}")
    except Exception as e:
        print(f"Failed to save enriched data: {e}")

def save_failed_fetches(failed_fetches: List[Dict[str, Any]], 
                       output_file: str = 'failed_epic_fetches.json'):
    """
    Save failed Epic fetches (API errors only) to JSON file.
    Games that simply don't exist on Epic Store are not included.
    
    Args:
        failed_fetches (List[Dict]): List of failed fetches (API errors)
        output_file (str): Output file path
    """
    if failed_fetches:
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(failed_fetches, f, indent=2, ensure_ascii=False)
            # print(f"Failed fetches saved to {output_file} ({len(failed_fetches)} API errors)")
        except Exception as e:
            print(f"Failed to save failed fetches: {e}")

def print_enrichment_summary(enriched_games: List[Dict[str, Any]], 
                            failed_fetches: List[Dict[str, Any]]):
    """
    Print a summary of the enrichment process.
    
    Args:
        enriched_games (List[Dict]): Enriched games data
        failed_fetches (List[Dict]): List of failed fetches (API errors only)
    """
    total_games = len(enriched_games)
    games_with_epic_info = sum(1 for game in enriched_games if 'epicInfo' in game)
    games_not_on_epic = total_games - games_with_epic_info - len(failed_fetches)
    
    print("\n" + "="*60)
    print("IGDB-EPIC ENRICHMENT SUMMARY")
    print("="*60)
    print(f"Total IGDB games processed: {total_games:,}")
    print(f"Games enriched with Epic data: {games_with_epic_info:,}")
    print(f"Games not found on Epic Store: {games_not_on_epic:,}")
    print(f"Failed fetches (API errors): {len(failed_fetches):,}")
    print(f"Enrichment rate: {(games_with_epic_info/total_games*100):.1f}%")
    
    if games_with_epic_info > 0:
        print(f"\nSample of enriched games:")
        count = 0
        for game in enriched_games:
            if 'epicInfo' in game and count < 5:
                epic_info = game['epicInfo']
                igdb_name = game.get('name', 'Unknown')
                epic_title = epic_info.get('title', 'Unknown')
                epic_price = epic_info.get('price', {}).get('totalPrice', {}).get('fmtPrice', {}).get('originalPrice', 'Free')
                print(f"{count+1}. IGDB: '{igdb_name}' -> Epic: '{epic_title}' (Price: {epic_price})")
                count += 1
    
    print("="*60)

if __name__ == "__main__":
    print("IGDB-Epic Enrichment Tool")
    print("Enriching IGDB games with Epic data from Epic Games Store API...")
    print("="*60)
    
    # Load data
    igdb_games = load_igdb_games()
    if not igdb_games:
        print("Failed to load IGDB games. Exiting.")
        exit(1)
    
    print(f"\nFound {len(igdb_games):,} IGDB games to process.")
    print("Note: This will make API calls to Epic Games Store, which may take a while.")
    print(f"Processing in batches of {BATCH_SIZE} games. Progress will be saved after each batch.\n")
    
    # Enrich games
    enriched_games, failed_fetches = enrich_igdb_with_epic_data(igdb_games)
    
    # Save final results
    if enriched_games:
        save_enriched_data(enriched_games, output_file='data/igdb_games_enriched_final.json')
        save_failed_fetches(failed_fetches)
        print_enrichment_summary(enriched_games, failed_fetches)
    else:
        print("No games were processed successfully.")