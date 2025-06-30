import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
from pathlib import Path

class CreateHosts:
    def __init__(self, db_host="localhost", db_user="postgres", db_password="", db_port=5432):
        self.db_config = {
            'host': db_host,
            'user': db_user, 
            'password': db_password,
            'port': db_port
        }
        
        # Host相关字段
        self.host_columns = [
            'host_id', 'host_url', 'host_name', 'host_since', 'host_location', 
            'host_about', 'host_response_time', 'host_response_rate', 
            'host_acceptance_rate', 'host_is_superhost', 'host_thumbnail_url', 
            'host_picture_url', 'host_neighbourhood', 'host_listings_count', 
            'host_total_listings_count', 'host_verifications', 'host_has_profile_pic', 
            'host_identity_verified', 'calculated_host_listings_count',
            'calculated_host_listings_count_entire_homes', 
            'calculated_host_listings_count_private_rooms',
            'calculated_host_listings_count_shared_rooms'
        ]
    
    def get_all_listings_tables(self, engine):
        """获取所有城市的listings表"""
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT schemaname, tablename 
                FROM pg_tables 
                WHERE schemaname != 'information_schema' 
                AND schemaname != 'pg_catalog'
                AND tablename LIKE 'listings_%'
                ORDER BY schemaname, tablename
            """))
            return result.fetchall()
    
    def table_exists(self, engine, schema_name, table_name):
        """Check if table exists"""
        with engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = '{schema_name}' 
                AND table_name = '{table_name}'
            """))
            return result.fetchone() is not None
    
    def extract_hosts_from_listings(self, engine, schema_name, listings_table):
        """从listings表提取hosts数据"""
        
        # 构建查询，只选择存在的host字段
        available_columns = []
        with engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = '{schema_name}' 
                AND table_name = '{listings_table}'
            """))
            existing_cols = [row[0] for row in result.fetchall()]
        
        # 找出实际存在的host字段
        for col in self.host_columns:
            if col in existing_cols:
                available_columns.append(col)
        
        if not available_columns:
            print(f"    No host columns found in {schema_name}.{listings_table}")
            return None
        
        # 构建SQL查询
        columns_str = ', '.join(available_columns)
        sql = f"""
            SELECT DISTINCT {columns_str}
            FROM {schema_name}.{listings_table}
            WHERE host_id IS NOT NULL
        """
        
        # 执行查询
        df = pd.read_sql(sql, engine)
        
        # 去重（以host_id为基准）
        if 'host_id' in df.columns:
            df = df.drop_duplicates(subset=['host_id'])
        
        return df
    
    def create_hosts_table(self, engine, schema_name, hosts_table, df):
        """创建hosts表并设置主键"""
        # 创建表
        df.to_sql(hosts_table, engine, schema=schema_name, if_exists='replace', index=False)
        
        # 添加主键约束
        if 'host_id' in df.columns:
            try:
                with engine.connect() as conn:
                    conn.execute(text(f"""
                        ALTER TABLE {schema_name}.{hosts_table} 
                        ADD CONSTRAINT {hosts_table}_pk PRIMARY KEY (host_id)
                    """))
                    conn.commit()
            except Exception as e:
                print(f"    Warning: Could not add primary key: {e}")
    
    def process_all_cities(self):
        """处理所有城市的listings表"""
        engine = create_engine(f"postgresql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/ia_detail")
        
        # 获取所有listings表
        tables = self.get_all_listings_tables(engine)
        
        if not tables:
            print("No listings tables found in ia_detail database")
            return
        
        print(f"Found {len(tables)} listings tables")
        
        for schema_name, listings_table in tables:
            # 提取日期（假设表名格式为 listings_YYYY_MM_DD）
            date_part = listings_table.replace('listings_', '')
            hosts_table = f"hosts_{date_part}"
            
            # 检查hosts表是否已存在
            if self.table_exists(engine, schema_name, hosts_table):
                print(f"Skipping {schema_name}.{hosts_table} (already exists)")
                continue
            
            print(f"Processing {schema_name}.{listings_table} -> {hosts_table}")
            
            try:
                # 提取hosts数据
                hosts_df = self.extract_hosts_from_listings(engine, schema_name, listings_table)
                
                if hosts_df is None or hosts_df.empty:
                    print(f"    No host data found")
                    continue
                
                # 创建hosts表
                self.create_hosts_table(engine, schema_name, hosts_table, hosts_df)
                
                print(f"    Created {schema_name}.{hosts_table} with {len(hosts_df)} hosts")
                
            except Exception as e:
                print(f"    Error processing {schema_name}.{listings_table}: {e}")

# Create global instance
_createhosts = CreateHosts()

def createhosts(db_host="localhost", db_user="postgres", db_password="", db_port=5432):
    """Create hosts tables from listings_detail tables"""
    processor = CreateHosts(db_host, db_user, db_password, db_port)
    processor.process_all_cities()