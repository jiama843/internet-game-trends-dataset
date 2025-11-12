import os
import json
import time
import requests
from typing import List, Dict, Any
from dotenv import load_dotenv

load_dotenv()

# # Configuration - Replace with your actual credentials or set as environment variables
CLIENT_ID = os.getenv('TWITCH_CLIENT_ID')
CLIENT_SECRET = os.getenv('TWITCH_CLIENT_SECRET')

# This will need to be replaced each time it expires
CLIENT_ACCESS_TOKEN = os.getenv('TWITCH_ACCESS_TOKEN')

# # API endpoints
TWITCH_TOKEN_URL = 'https://id.twitch.tv/oauth2/token'
IGDB_BASE_URL = 'https://api.igdb.com/v4'

REQUEST_DELAY = 0.05  # Delay between API requests to be respectful

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
    
    def _filter_batch(self, games: List[Dict[Any, Any]]) -> List[Dict[Any, Any]]:
        """
        Filter a batch of games to only include those with exactly one perspective, 
        one game_engine, and one developer.
        
        Args:
            games (List[Dict]): Batch of game data
            
        Returns:
            List[Dict]: Filtered batch of games
        """
        filtered_games = []
        
        for game in games:
            # Check if has exactly one player_perspective
            perspectives = game.get('player_perspectives', [])
            has_one_perspective = len(perspectives) == 1
            
            # Check if has exactly one game_engine
            engines = game.get('game_engines', [])
            has_one_engine = len(engines) == 1
            
            # Check if has exactly one developer
            companies = game.get('involved_companies', [])
            developers = [c for c in companies if c.get('developer', False)]
            has_one_developer = len(developers) == 1
            
            # Include game if all conditions are met
            if has_one_perspective and has_one_engine and has_one_developer:
                filtered_games.append(game)
        
        return filtered_games

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
            print("Not authenticated. Call authenticate() first.")
            return []
        
        all_games = []
        batch_size = min(batch_size, 500)
        offset = 0
        
        print(f"Fetching games (max: {'unlimited' if max_games is None else max_games})...")
        
        while True:
            # Stop if we've reached the maximum number of games
            if max_games is not None and len(all_games) >= max_games:
                print(f"Reached maximum limit of {max_games} games")
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
                 game_engines.id,
                 game_engines.name,
                 external_games.category,
                 external_games.uid,
                 external_games.external_game_source.name,
                 involved_companies.id,
                 involved_companies.company.name,
                 involved_companies.developer;
             where (rating_count >= 100 | aggregated_rating_count >= 1) & game_type.id = 0 
                 & player_perspectives != null
                 & game_engines != null
                 & involved_companies != null;
             limit {current_batch_size};
             offset {offset};
             """
            
            print(f"Fetching batch {offset//batch_size + 1}: games {offset+1}-{offset+current_batch_size}")
            
            games_batch = self.make_api_request('games', query)
            
            if not games_batch:
                print(f"No games returned for offset {offset}")
                break
            
            # Filter batch immediately to only include games with exactly 1 perspective, 1 engine, and 1 developer
            filtered_games_batch = self._filter_batch(games_batch)

            all_games.extend(filtered_games_batch)
            
            # Rate limiting - up to 4 requests per second
            time.sleep(REQUEST_DELAY)
            
            # If we got fewer games than requested, we've reached the end
            if len(games_batch) < current_batch_size:
                print("Received fewer games than requested, likely reached end of data")
                break
            
            # Move to next batch
            offset += current_batch_size
        
        print(f"Successfully fetched {len(all_games)} games")
        return all_games
    
    def save_to_json(self, data: List[Dict[Any, Any]], filename: str):
        """
        Save fetched data to JSON file.
        
        Args:
            data (List[Dict]): Game data to save
            filename (str): Output filename
        """
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            print(f"Data saved to {filename}")
        except Exception as e:
            print(f"Failed to save data: {e}")

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
            controller.save_to_json(games, 'data/igdb_games.json')
            
            # Print summary
            print(f"Fetched {len(games)} games successfully!")
            print("Sample of top 5 games:")
            for i, game in enumerate(games[:5]):
                name = game.get('name', 'Unknown')
                print(f"{i+1}. {name}")
        else:
            print("No games were fetched")
            
    except Exception as e:
        print(f"Error during execution: {e}")

if __name__ == '__main__':
    main()
