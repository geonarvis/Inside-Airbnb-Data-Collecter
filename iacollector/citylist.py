import requests
from bs4 import BeautifulSoup
import re
from typing import Dict

class CityList:
    def __init__(self):
        self.url = "https://insideairbnb.com/get-the-data/"
        
    def get_cities(self) -> Dict[str, str]:
        """Get all cities and their latest dates"""
        try:
            soup = BeautifulSoup(requests.get(self.url).content, 'html.parser')
            cities = {}
            
            for h3 in soup.find_all('h3'):
                city = h3.get_text().strip()
                if any(word in city.lower() for word in ['get the data', 'archived', 'contact']):
                    continue
                    
                dates = self._find_dates(h3)
                if dates:
                    cities[city] = max(dates)
            
            return cities
        except:
            return {}
    
    def _find_dates(self, header):
        """Find dates after the header"""
        dates = []
        current = header
        
        for _ in range(10):
            current = current.next_sibling
            if not current or current.name == 'h3':
                break
                
            text = current.get_text() if hasattr(current, 'get_text') else str(current)
            
            dates.extend(re.findall(r'\d{4}-\d{2}-\d{2}', text))
            
            for day, month, year in re.findall(r'(\d{1,2})\s+([A-Za-z]+),?\s+(\d{4})', text):
                months = {'january':1, 'february':2, 'march':3, 'april':4, 'may':5, 'june':6,
                         'july':7, 'august':8, 'september':9, 'october':10, 'november':11, 'december':12}
                if month.lower() in months:
                    dates.append(f"{year}-{months[month.lower()]:02d}-{int(day):02d}")
        
        return dates
    
    def print_table(self, cities: Dict[str, str]):
        """Print city table"""
        print(f"{'City':<40} {'Date':<12}")
        print("-" * 55)
        for city, date in sorted(cities.items()):
            print(f"{city:<40} {date:<12}")

# Create global instance
_citylist = CityList()

def citylist():
    """Get and display all available cities"""
    cities = _citylist.get_cities()
    if cities:
        _citylist.print_table(cities)
        print(f"\nTotal: {len(cities)} cities")
        return cities
    else:
        print("Failed to get city data")
        return {}