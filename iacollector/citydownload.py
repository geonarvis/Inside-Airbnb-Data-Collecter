import requests
from bs4 import BeautifulSoup
import re
from pathlib import Path
from typing import Dict, List, Union
from urllib.parse import urlparse

class CityDownloader:
    def __init__(self):
        self.base_url = "https://data.insideairbnb.com"
        self.get_data_url = "https://insideairbnb.com/get-the-data/"
        
        # 分类数据类型：data文件夹 vs visualisations文件夹
        self.data_files = [
            'listings.csv.gz',      # Detailed Listings data
            'reviews.csv.gz',       # Detailed Review Data
            'calendar.csv.gz',      # Detailed Calendar Data
        ]
        
        self.visualisation_files = [
            'listings.csv',         # Summary information (for visualisations)
            'reviews.csv',          # Summary Review data
            'neighbourhoods.csv',   # Neighbourhood list for geo filter
            'neighbourhoods.geojson' # GeoJSON file of neighbourhoods
        ]
        
        self._city_mappings = None
        
    def _get_city_mappings(self) -> Dict[str, Dict]:
        """Get city display name to URL path mapping"""
        if self._city_mappings is not None:
            return self._city_mappings
            
        print("Parsing city mappings...")
        try:
            soup = BeautifulSoup(requests.get(self.get_data_url).content, 'html.parser')
            mappings = {}
            
            for h3 in soup.find_all('h3'):
                city_display_name = h3.get_text().strip()
                if any(word in city_display_name.lower() for word in ['get the data', 'archived', 'contact']):
                    continue
                
                url_path = self._find_city_url_path(h3)
                latest_date = self._find_latest_date(h3)
                
                if url_path and latest_date:
                    mappings[city_display_name] = {
                        'url_path': url_path,
                        'latest_date': latest_date,
                        'city_folder': self._get_city_folder_name(city_display_name)
                    }
            
            self._city_mappings = mappings
            print(f"Found {len(mappings)} city mappings")
            return mappings
            
        except Exception as e:
            print(f"Failed to get city mappings: {e}")
            return {}
    
    def _find_city_url_path(self, header):
        """Find URL path after city header"""
        current = header
        
        for _ in range(15):
            current = current.next_sibling
            if not current or current.name == 'h3':
                break
            
            if hasattr(current, 'find_all'):
                links = current.find_all('a', href=True)
                for link in links:
                    href = link['href']
                    if 'data.insideairbnb.com' in href:
                        return self._extract_url_path(href)
        
        return None
    
    def _extract_url_path(self, url: str) -> str:
        """Extract city path from full URL"""
        try:
            parsed = urlparse(url)
            path_parts = [p for p in parsed.path.split('/') if p]
            
            if len(path_parts) >= 5 and path_parts[-2] == 'data':
                return '/'.join(path_parts[:3])
            
            return None
        except:
            return None
    
    def _find_latest_date(self, header) -> str:
        """Find latest date"""
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
        
        return max(dates) if dates else ""
    
    def _get_city_folder_name(self, display_name: str) -> str:
        """Generate folder name from display name"""
        city_name = display_name.split(',')[0].strip()
        return city_name.lower().replace(' ', '_').replace('-', '_')
    
    def _find_matching_cities(self, city_names: List[str]) -> Dict[str, Dict]:
        """Find matching cities based on input city names"""
        mappings = self._get_city_mappings()
        matches = {}
        
        for input_name in city_names:
            input_lower = input_name.lower()
            found = False
            
            for display_name, info in mappings.items():
                if (input_lower in display_name.lower() or 
                    input_lower in info['city_folder'] or
                    any(input_lower in part.lower() for part in display_name.split(','))):
                    matches[input_name] = {
                        'display_name': display_name,
                        **info
                    }
                    found = True
                    break
            
            if not found:
                print(f"City not found: {input_name}")
        
        return matches
    
    def download_cities(self, 
                       city_names: Union[str, List[str]], 
                       output_dir: str = "airbnb_data",
                       force_download: bool = False,
                       downloadpath: str = "all") -> Dict[str, Dict]:
        """Download data for specified cities"""
        
        if isinstance(city_names, str):
            city_names = [city_names]
        
        # 验证 downloadpath 参数
        valid_paths = ["data", "visualisations", "all"]
        if downloadpath not in valid_paths:
            raise ValueError(f"downloadpath must be one of {valid_paths}")
        
        Path(output_dir).mkdir(exist_ok=True)
        
        matched_cities = self._find_matching_cities(city_names)
        
        if not matched_cities:
            print("No matching cities found")
            return {}
        
        results = {}
        
        for input_name, city_info in matched_cities.items():
            print(f"\nDownloading {city_info['display_name']} (path: {downloadpath})...")
            
            try:
                downloaded_files = self._download_single_city(city_info, output_dir, force_download, downloadpath)
                results[input_name] = {
                    'display_name': city_info['display_name'],
                    'files': downloaded_files,
                    'status': 'success'
                }
                
            except Exception as e:
                print(f"Download failed: {e}")
                results[input_name] = {
                    'display_name': city_info['display_name'],
                    'files': {},
                    'status': 'failed',
                    'error': str(e)
                }
        
        return results
    
    def _download_single_city(self, city_info: Dict, output_dir: str, force_download: bool = False, downloadpath: str = "all") -> Dict[str, str]:
        """Download data for a single city"""
        url_path = city_info['url_path']
        date = city_info['latest_date']
        folder_name = city_info['city_folder']
        
        city_dir = Path(output_dir) / folder_name / date
        city_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"Saving to: {city_dir}")
        
        downloaded_files = {}
        success_count = 0
        skipped_count = 0
        
        # 根据 downloadpath 参数决定下载哪些文件
        files_to_download = []
        
        if downloadpath in ["data", "all"]:
            files_to_download.extend([(f, "data") for f in self.data_files])
        
        if downloadpath in ["visualisations", "all"]:
            files_to_download.extend([(f, "visualisations") for f in self.visualisation_files])
        
        total_files = len(files_to_download)
        
        # 下载文件
        for filename, folder_type in files_to_download:
            result = self._download_file(url_path, date, filename, city_dir, folder_type, force_download)
            if result == "success":
                downloaded_files[filename] = str(city_dir / filename)
                success_count += 1
            elif result == "skipped":
                downloaded_files[filename] = str(city_dir / filename)
                skipped_count += 1
        
        print(f"Downloaded: {success_count}, Skipped: {skipped_count}, Failed: {total_files - success_count - skipped_count}")
        return downloaded_files
    
    def _download_file(self, url_path: str, date: str, filename: str, city_dir: Path, folder_type: str, force_download: bool = False) -> str:
        """Download a single file from either data or visualisations folder"""
        file_path = city_dir / filename
        
        # 检查文件是否已存在
        if file_path.exists() and not force_download:
            print(f"  Skipped {filename} (already exists)")
            return "skipped"
        
        try:
            download_url = f"{self.base_url}/{url_path}/{date}/{folder_type}/{filename}"
            
            print(f"  Downloading {filename} from {folder_type}/...")
            
            response = requests.get(download_url, stream=True, timeout=30)
            response.raise_for_status()
            
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            print(f"  Success: {filename}")
            return "success"
            
        except Exception as e:
            print(f"  Failed {filename}: {e}")
            return "failed"

# Create global instance
_downloader = CityDownloader()

def citydownload(city_names: Union[str, List[str]], output_dir: str = "airbnb_data", force_download: bool = False, downloadpath: str = "all"):
    """Download data for specified cities"""
    return _downloader.download_cities(city_names, output_dir, force_download, downloadpath)