import os
import gzip
import pandas as pd
import psycopg2
from pathlib import Path
from sqlalchemy import create_engine, text
import re

class ToSQL:
    def __init__(self, db_host="localhost", db_user="postgres", db_password="", db_port=5432):
        self.db_config = {
            'host': db_host,
            'user': db_user, 
            'password': db_password,
            'port': db_port
        }
        
        # 定义精选字段列表
        self.selected_detail_columns = [
            'id', 'host_id', 'host_response_time', 'host_response_rate', 'host_acceptance_rate',
            'host_is_superhost', 'host_neighbourhood', 'host_listings_count', 'host_total_listings_count',
            'neighbourhood_cleansed', 'latitude', 'longitude', 'property_type', 'room_type',
            'accommodates', 'bathrooms', 'bedrooms', 'beds', 'price', 'minimum_nights',
            'maximum_nights', 'minimum_nights_avg_ntm', 'maximum_nights_avg_ntm',
            'availability_365', 'availability_30', 'availability_60', 'availability_90',
            'number_of_reviews', 'number_of_reviews_ltm', 'number_of_reviews_l30d',
            'number_of_reviews_ly', 'estimated_occupancy_l365d', 'estimated_revenue_l365d',
            'first_review', 'last_review', 'review_scores_rating', 'review_scores_accuracy',
            'review_scores_cleanliness', 'review_scores_checkin', 'review_scores_communication',
            'review_scores_location', 'review_scores_value', 'instant_bookable',
            'calculated_host_listings_count', 'calculated_host_listings_count_entire_homes',
            'calculated_host_listings_count_private_rooms', 'calculated_host_listings_count_shared_rooms',
            'reviews_per_month'
        ]
        
    def create_database_if_not_exists(self, db_name):
        """Create database if it doesn't exist"""
        conn = psycopg2.connect(**self.db_config, database='postgres')
        conn.autocommit = True
        cur = conn.cursor()
        
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")
        if not cur.fetchone():
            cur.execute(f"CREATE DATABASE {db_name}")
            print(f"Created database: {db_name}")
        
        cur.close()
        conn.close()
    
    def create_schema_if_not_exists(self, engine, schema_name):
        """Create schema if it doesn't exist"""
        with engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
            conn.commit()
    
    def table_exists(self, engine, schema_name, table_name):
        """Check if table exists"""
        with engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = '{schema_name}' 
                AND table_name = '{table_name}'
            """))
            return result.fetchone() is not None
    
    def clean_price_field(self, price_series):
        """Clean price field: remove currency symbols and convert to float"""
        def clean_price(price):
            if pd.isna(price):
                return None
            
            # Convert to string if not already
            price_str = str(price)
            
            # Remove currency symbols and common formatting
            price_clean = re.sub(r'[\$,€£¥₹]', '', price_str)
            price_clean = price_clean.strip()
            
            try:
                return float(price_clean)
            except (ValueError, TypeError):
                return None
        
        return price_series.apply(clean_price)
    
    def clean_date_field(self, date_series):
        """Clean date field: convert to YYYY-MM-DD format"""
        def clean_date(date_val):
            if pd.isna(date_val):
                return None
            
            try:
                # Use pandas to_datetime which handles many formats
                parsed_date = pd.to_datetime(date_val, errors='coerce')
                if pd.isna(parsed_date):
                    return None
                return parsed_date.strftime('%Y-%m-%d')
            except:
                return None
        
        return date_series.apply(clean_date)
    
    def filter_selected_columns(self, df, selected_columns):
        """Filter dataframe to only include selected columns that exist"""
        available_columns = [col for col in selected_columns if col in df.columns]
        missing_columns = [col for col in selected_columns if col not in df.columns]
        
        if missing_columns:
            print(f"    Note: {len(missing_columns)} columns not found in data")
        
        return df[available_columns].copy()
    
    def process_listings_detail_dataframe(self, df, use_selected_detail=False):
        """Process listings detail dataframe with field transformations"""
        
        # 1. 如果启用精选字段，先筛选列
        if use_selected_detail:
            df_processed = self.filter_selected_columns(df, self.selected_detail_columns)
            print(f"    Selected {len(df_processed.columns)} out of {len(df.columns)} columns")
        else:
            df_processed = df.copy()
        
        # 2. Rename columns
        column_renames = {
            'id': 'listing_id',
            'name': 'listing_name',
            'description': 'listing_description'
        }
        
        for old_col, new_col in column_renames.items():
            if old_col in df_processed.columns:
                df_processed = df_processed.rename(columns={old_col: new_col})
        
        # 3. Clean price field
        if 'price' in df_processed.columns:
            df_processed['price'] = self.clean_price_field(df_processed['price'])
        
        # 4. Clean date fields
        date_fields = ['last_scraped', 'host_since', 'calendar_last_scraped', 
                      'first_review', 'last_review']
        
        for date_field in date_fields:
            if date_field in df_processed.columns:
                df_processed[date_field] = self.clean_date_field(df_processed[date_field])
        
        return df_processed
    
    def process_listings_simple_dataframe(self, df):
        """Process listings simple dataframe with field transformations"""
        df_processed = df.copy()
        
        # Rename columns for simple listings
        column_renames = {
            'id': 'listing_id',
            'name': 'listing_name'
        }
        
        for old_col, new_col in column_renames.items():
            if old_col in df_processed.columns:
                df_processed = df_processed.rename(columns={old_col: new_col})
        
        return df_processed
    
    def create_table_with_primary_key(self, engine, schema_name, table_name, df, pk_column):
        """Create table with primary key constraint"""
        # First create the table normally
        df.to_sql(table_name, engine, schema=schema_name, if_exists='fail', index=False)
        
        # Then add primary key constraint
        try:
            with engine.connect() as conn:
                conn.execute(text(f"""
                    ALTER TABLE {schema_name}.{table_name} 
                    ADD CONSTRAINT {table_name}_pk PRIMARY KEY ({pk_column})
                """))
                conn.commit()
        except Exception as e:
            print(f"    Warning: Could not add primary key constraint: {e}")
    
    def decompress_gz_files(self, data_dir="airbnb_data"):
        """Decompress all .gz files with _detail suffix"""
        data_path = Path(data_dir)
        
        for city_folder in data_path.iterdir():
            if not city_folder.is_dir():
                continue
                
            for date_folder in city_folder.iterdir():
                if not date_folder.is_dir():
                    continue
                    
                for gz_file in date_folder.glob("*.gz"):
                    # 修复：正确处理文件名
                    base_name = gz_file.name.replace('.csv.gz', '').replace('.gz', '')
                    csv_file = date_folder / f"{base_name}_detail.csv"
                    
                    if not csv_file.exists():
                        print(f"Decompressing {gz_file} -> {csv_file.name}")
                        with gzip.open(gz_file, 'rb') as f_in:
                            with open(csv_file, 'wb') as f_out:
                                f_out.write(f_in.read())
    
    def process_detail_data(self, data_dir="airbnb_data", include_calendar=False, use_selected_detail=False):
        """Process all detail files to ia_detail database"""
        self.create_database_if_not_exists('ia_detail')
        engine = create_engine(f"postgresql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/ia_detail")
        
        data_path = Path(data_dir)
        
        for city_folder in data_path.iterdir():
            if not city_folder.is_dir():
                continue
                
            city_name = city_folder.name
            self.create_schema_if_not_exists(engine, city_name)
            
            for date_folder in city_folder.iterdir():
                if not date_folder.is_dir():
                    continue
                    
                date_str = date_folder.name.replace('-', '_')
                
                # 只处理 _detail.csv 文件
                for csv_file in date_folder.glob("*_detail.csv"):
                    # 从文件名提取数据类型 (例如: listings_detail.csv -> listings)
                    data_type = csv_file.stem.replace('_detail', '')
                    
                    # 根据参数决定是否跳过 calendar
                    if data_type == 'calendar' and not include_calendar:
                        print(f"Skipping {csv_file.name} (calendar disabled)")
                        continue
                    
                    table_name = f"{data_type}_{date_str}"
                    
                    # 检查表是否已存在
                    if self.table_exists(engine, city_name, table_name):
                        print(f"Skipping {city_name}.{table_name} (already exists)")
                        continue
                    
                    print(f"Loading {city_name}.{table_name}")
                    
                    try:
                        df = pd.read_csv(csv_file, low_memory=False)
                        
                        # 特殊处理 listings 表
                        if data_type == 'listings':
                            df = self.process_listings_detail_dataframe(df, use_selected_detail)
                            self.create_table_with_primary_key(engine, city_name, table_name, df, 'listing_id')
                        else:
                            # 其他表正常处理
                            df.to_sql(table_name, engine, schema=city_name, if_exists='fail', index=False)
                            
                    except Exception as e:
                        print(f"Error loading {csv_file}: {e}")
    
    def process_simple_data(self, data_dir="airbnb_data"):
        """Process reviews.csv and listings.csv to ia_simple database"""
        self.create_database_if_not_exists('ia_simple')
        engine = create_engine(f"postgresql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/ia_simple")
        
        data_path = Path(data_dir)
        
        for city_folder in data_path.iterdir():
            if not city_folder.is_dir():
                continue
                
            city_name = city_folder.name
            self.create_schema_if_not_exists(engine, city_name)
            
            for date_folder in city_folder.iterdir():
                if not date_folder.is_dir():
                    continue
                    
                date_str = date_folder.name.replace('-', '_')
                
                # 只处理原始的 reviews.csv 和 listings.csv (不是 _detail 版本)
                for filename in ['reviews.csv', 'listings.csv']:
                    csv_file = date_folder / filename
                    if csv_file.exists():
                        data_type = csv_file.stem
                        table_name = f"{data_type}_{date_str}"
                        
                        # 检查表是否已存在
                        if self.table_exists(engine, city_name, table_name):
                            print(f"Skipping {city_name}.{table_name} (already exists)")
                            continue
                        
                        print(f"Loading {city_name}.{table_name}")
                        
                        try:
                            df = pd.read_csv(csv_file, low_memory=False)
                            
                            # 特殊处理 listings 表
                            if data_type == 'listings':
                                df = self.process_listings_simple_dataframe(df)
                                self.create_table_with_primary_key(engine, city_name, table_name, df, 'listing_id')
                            else:
                                # reviews 表正常处理
                                df.to_sql(table_name, engine, schema=city_name, if_exists='fail', index=False)
                                
                        except Exception as e:
                            print(f"Error loading {csv_file}: {e}")
    
    def run(self, data_dir="airbnb_data", include_calendar=False, use_selected_detail=False):
        """Run the complete process"""
        print("Step 1: Decompressing .gz files...")
        self.decompress_gz_files(data_dir)
        
        selection_status = "selected columns" if use_selected_detail else "all columns"
        print(f"\nStep 2: Loading detail data to ia_detail database (calendar: {'enabled' if include_calendar else 'disabled'}, columns: {selection_status})...")
        self.process_detail_data(data_dir, include_calendar, use_selected_detail)
        
        print("\nStep 3: Loading simple data to ia_simple database...")
        self.process_simple_data(data_dir)
        
        print("\nDone!")

# Create global instance
_tosql = ToSQL()

def tosql(db_host="localhost", db_user="postgres", db_password="", db_port=5432, data_dir="airbnb_data", include_calendar=False, selected_detail=False):
    """Import airbnb data to PostgreSQL"""
    
    processor = ToSQL(db_host, db_user, db_password, db_port)
    processor.run(data_dir, include_calendar, selected_detail)