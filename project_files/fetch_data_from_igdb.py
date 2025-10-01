import os
import json
import time
import requests
from typing import List, Dict, Any
import logging
from dotenv import load_dotenv

# # Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

# # Configuration - Replace with your actual credentials or set as environment variables
CLIENT_ID = os.getenv('TWITCH_CLIENT_ID')
CLIENT_SECRET = os.getenv('TWITCH_CLIENT_SECRET')

# This will need to be replaced each time it expires
CLIENT_ACCESS_TOKEN = os.getenv('TWITCH_ACCESS_TOKEN')

# # API endpoints
TWITCH_TOKEN_URL = 'https://id.twitch.tv/oauth2/token'
IGDB_BASE_URL = 'https://api.igdb.com/v4'

class IGDBController:
    """Class to handle IGDB API authentication and data fetching."""
    
    def __init__(self, client_id: str, client_secret: str, client_access_token: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = client_access_token
        self.headers = {
            'Client-ID': self.client_id,
            'Authorization': f'Bearer {self.access_token}',
            'Accept': 'application/json'
        }

    def make_api_request(self, endpoint: str, query: str) -> List[Dict[Any, Any]]:
        """
        Make a request to the IGDB API.
        
        Args:
            endpoint (str): API endpoint (e.g., 'games')
            query (str): IGDB query string

        Returns:
            List[Dict]: JSON response from API
        """
        url = f"{IGDB_BASE_URL}/{endpoint}"

        response = requests.post(url, **{ 'headers': self.headers, 'data': query })

        # print(response.text)
        return response.json()

    def fetch_games(self, max_games: int = None, batch_size: int = 500) -> List[Dict[Any, Any]]:
        """
        Fetch games until no more entries are available or max_games is reached.
        
        Args:
            max_games (int, optional): Maximum number of games to fetch. If None, fetch all available.
            batch_size (int): Number of games per API request (max 500)
        Returns:
            List[Dict]: List of game data
        """
        if not self.access_token:
            logger.error("Not authenticated. Call authenticate() first.")
            return []
        
        all_games = []
        batch_size = min(batch_size, 500)
        offset = 0
        
        logger.info(f"Fetching games (max: {'unlimited' if max_games is None else max_games})...")
        
        while True:
            # Stop if we've reached the maximum number of games
            if max_games is not None and len(all_games) >= max_games:
                logger.info(f"Reached maximum limit of {max_games} games")
                break
                
            # Calculate batch size for this request
            if max_games is not None:
                current_batch_size = min(batch_size, max_games - len(all_games))
            else:
                current_batch_size = batch_size
            
            # IGDB query to get games with comprehensive data
            query = f"""
             fields 
                 id,
                 aggregated_rating,
                 aggregated_rating_count,
                 first_release_date,
                 player_perspectives.id,
                 player_perspectives.name,
                 rating,
                 rating_count,
                 name,
                 themes.name,
                 themes.id,
                 game_modes.name,
                 game_modes.id,
                 genres.name,
                 genres.id,
                 hypes,
                 external_games.category,
                 external_games.uid,
                 external_games.external_game_source.name;
             where (rating_count >= 100 | aggregated_rating_count >= 1) & game_type.id = 0;
             limit {current_batch_size};
             offset {offset};
             """
            
            logger.info(f"Fetching batch {offset//batch_size + 1}: games {offset+1}-{offset+current_batch_size}")
            
            games_batch = self.make_api_request('games', query)
            
            if not games_batch:
                logger.warning(f"No games returned for offset {offset}")
                break
            
            all_games.extend(games_batch)
            
            # Rate limiting - up to 4 requests per second
            time.sleep(0.25)
            
            # If we got fewer games than requested, we've reached the end
            if len(games_batch) < current_batch_size:
                logger.info("Received fewer games than requested, likely reached end of data")
                break
            
            # Move to next batch
            offset += current_batch_size
        
        logger.info(f"Successfully fetched {len(all_games)} games")
        return all_games
    
    def save_to_json(self, data: List[Dict[Any, Any]], filename: str = 'igdb_top_games.json'):
        """
        Save fetched data to JSON file.
        
        Args:
            data (List[Dict]): Game data to save
            filename (str): Output filename
        """
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.info(f"Data saved to {filename}")
        except Exception as e:
            logger.error(f"Failed to save data: {e}")

def main():
    controller = IGDBController(CLIENT_ID, CLIENT_SECRET, CLIENT_ACCESS_TOKEN)
    
    # query = """
    #     fields *;
    #     where id = (361573, 11642, 369853);
    #     limit 10;
    # """

    # games = controller.make_api_request('games', query)

    # # Fetch top games
    try:
        games = controller.fetch_games(max_games=None, batch_size=500)  # Fetch all available games
        
        if games:
            # Save to JSON file
            controller.save_to_json(games, 'igdb_games.json')
            
            # Print summary
            logger.info(f"Fetched {len(games)} games successfully!")
            logger.info("Sample of top 5 games:")
            for i, game in enumerate(games[:5]):
                name = game.get('name', 'Unknown')
                logger.info(f"{i+1}. {name}")
        else:
            logger.error("No games were fetched")
            
    except Exception as e:
        logger.error(f"Error during execution: {e}")

if __name__ == '__main__':
    main()
