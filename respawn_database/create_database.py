"""
RespawnMetrics Database Creator
Creates SQLite database from merged gaming psychology datasets

Author: Reannon Gray
Date: June 2025
"""

import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# Setup paths
project_root = Path(__file__).parent.parent
merged_dir = project_root / "respawn_data_merged"
db_dir = project_root / "respawn_database"

# Create database directory
db_dir.mkdir(exist_ok=True)

print("RespawnMetrics Database Creator")
print("Author: Reannon Gray")
print("=" * 50)
print(f"Source: {merged_dir}")
print(f"Target: {db_dir}")
print("=" * 50)

class GamingDatabaseCreator:
    """Creates and manages the RespawnMetrics gaming psychology database"""
    
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = None
        self.tables_created = []
        self.connected = False
        
    def connect(self):
        """Connect to SQLite database"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.execute("PRAGMA foreign_keys = ON")
            self.connected = True
            print(f"[OK] Connected to database: {self.db_path}")
            return True
        except Exception as e:
            print(f"[ERROR] Database connection failed: {e}")
            self.connected = False
            return False
    
    def create_comprehensive_table(self):
        """Create main comprehensive gaming psychology table"""
        if not self.connected or self.conn is None:
            print("[ERROR] Database not connected")
            return False
            
        try:
            file_path = merged_dir / "comprehensive_gaming_dataset.csv"
            if not file_path.exists():
                print(f"[!] Comprehensive dataset not found: {file_path}")
                return False
            
            # Load comprehensive dataset
            df = pd.read_csv(file_path)
            print(f"[LOAD] Loading comprehensive dataset: {len(df)} records")
            print(f"       Columns: {', '.join(df.columns[:8])}{'...' if len(df.columns) > 8 else ''}")
            
            # Create table schema based on actual columns
            create_sql = """
            CREATE TABLE IF NOT EXISTS comprehensive_gaming_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                participant_id TEXT UNIQUE NOT NULL,
                gaming_hours_daily REAL,
                age INTEGER,
                gender TEXT,
                anxiety_score REAL,
                aggression_score REAL,
                openness_score REAL,
                conscientiousness_score REAL,
                extraversion_score REAL,
                agreeableness_score REAL,
                neuroticism_score REAL,
                gaming_addiction_score REAL,
                social_gaming_score REAL,
                preferred_genre TEXT,
                plays_multiplayer BOOLEAN,
                has_microtransactions BOOLEAN,
                gaming_hours_played REAL,
                gaming_wellbeing_score REAL,
                gaming_stress_level REAL,
                gaming_social_connection_score REAL,
                gaming_achievement_satisfaction REAL,
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            
            self.conn.execute(create_sql)
            
            # Insert data with proper column mapping
            columns_to_insert = []
            values_placeholder = []
            
            for col in df.columns:
                if col in ['participant_id', 'gaming_hours_daily', 'age', 'gender', 'anxiety_score', 
                          'aggression_score', 'openness_score', 'conscientiousness_score', 
                          'extraversion_score', 'agreeableness_score', 'neuroticism_score',
                          'gaming_addiction_score', 'social_gaming_score', 'preferred_genre',
                          'plays_multiplayer', 'has_microtransactions']:
                    columns_to_insert.append(col)
                    values_placeholder.append('?')
                elif col.startswith('gaming_'):
                    # Handle gaming metrics from wellbeing data
                    columns_to_insert.append(col)
                    values_placeholder.append('?')
            
            if columns_to_insert:
                insert_sql = f"""
                INSERT OR REPLACE INTO comprehensive_gaming_data 
                ({', '.join(columns_to_insert)}) 
                VALUES ({', '.join(values_placeholder)})
                """
                
                # Prepare data for insertion
                data_to_insert = []
                for _, row in df.iterrows():
                    row_data = []
                    for col in columns_to_insert:
                        value = row.get(col)
                        if pd.isna(value):
                            row_data.append(None)
                        else:
                            row_data.append(value)
                    data_to_insert.append(row_data)
                
                self.conn.executemany(insert_sql, data_to_insert)
                self.conn.commit()
                
                print(f"[SUCCESS] Comprehensive table created: {len(data_to_insert)} records inserted")
                self.tables_created.append(('comprehensive_gaming_data', len(data_to_insert)))
                return True
            else:
                print(f"[!] No valid columns found for comprehensive table")
                return False
                
        except Exception as e:
            print(f"[ERROR] Failed to create comprehensive table: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def create_wellbeing_table(self):
        """Create detailed gaming wellbeing table"""
        if not self.connected or self.conn is None:
            print("[ERROR] Database not connected")
            return False
            
        try:
            file_path = merged_dir / "wellbeing_merged.csv"
            if not file_path.exists():
                print(f"[!] Wellbeing dataset not found: {file_path}")
                return False
            
            df = pd.read_csv(file_path)
            print(f"[LOAD] Loading wellbeing data: {len(df)} records")
            
            create_sql = """
            CREATE TABLE IF NOT EXISTS gaming_wellbeing (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                game_title TEXT,
                hours_played REAL,
                wellbeing_score REAL,
                stress_level REAL,
                social_connection_score REAL,
                achievement_satisfaction REAL,
                gaming_session_length REAL,
                age INTEGER,
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            
            self.conn.execute(create_sql)
            
            # Insert wellbeing data
            columns = ['user_id', 'game_title', 'hours_played', 'wellbeing_score', 
                      'stress_level', 'social_connection_score', 'achievement_satisfaction',
                      'gaming_session_length', 'age']
            
            available_columns = [col for col in columns if col in df.columns]
            
            if available_columns:
                placeholders = ', '.join(['?'] * len(available_columns))
                insert_sql = f"""
                INSERT INTO gaming_wellbeing ({', '.join(available_columns)}) 
                VALUES ({placeholders})
                """
                
                data_to_insert = []
                for _, row in df.iterrows():
                    row_data = [row.get(col) for col in available_columns]
                    data_to_insert.append(row_data)
                
                self.conn.executemany(insert_sql, data_to_insert)
                self.conn.commit()
                
                print(f"[SUCCESS] Wellbeing table created: {len(data_to_insert)} records inserted")
                self.tables_created.append(('gaming_wellbeing', len(data_to_insert)))
                return True
                
        except Exception as e:
            print(f"[ERROR] Failed to create wellbeing table: {e}")
            return False
    
    def create_games_table(self):
        """Create Steam games metadata table"""
        if not self.connected or self.conn is None:
            print("[ERROR] Database not connected")
            return False
            
        try:
            file_path = merged_dir / "steam_games_merged.csv"
            if not file_path.exists():
                print(f"[!] Steam games dataset not found: {file_path}")
                return False
            
            df = pd.read_csv(file_path)
            print(f"[LOAD] Loading Steam games data: {len(df)} records")
            
            create_sql = """
            CREATE TABLE IF NOT EXISTS steam_games (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id TEXT UNIQUE,
                name TEXT,
                genre TEXT,
                developer TEXT,
                price REAL,
                metacritic_score REAL,
                player_count INTEGER,
                release_year INTEGER,
                is_multiplayer BOOLEAN,
                has_microtransactions BOOLEAN,
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            
            self.conn.execute(create_sql)
            
            # Insert games data
            columns = ['game_id', 'name', 'genre', 'developer', 'price', 
                      'metacritic_score', 'player_count', 'release_year',
                      'is_multiplayer', 'has_microtransactions']
            
            available_columns = [col for col in columns if col in df.columns]
            
            if available_columns:
                placeholders = ', '.join(['?'] * len(available_columns))
                insert_sql = f"""
                INSERT OR REPLACE INTO steam_games ({', '.join(available_columns)}) 
                VALUES ({placeholders})
                """
                
                data_to_insert = []
                for _, row in df.iterrows():
                    row_data = [row.get(col) for col in available_columns]
                    data_to_insert.append(row_data)
                
                self.conn.executemany(insert_sql, data_to_insert)
                self.conn.commit()
                
                print(f"[SUCCESS] Steam games table created: {len(data_to_insert)} records inserted")
                self.tables_created.append(('steam_games', len(data_to_insert)))
                return True
                
        except Exception as e:
            print(f"[ERROR] Failed to create games table: {e}")
            return False
    
    def create_participant_tables(self):
        """Create detailed participant data tables"""
        if not self.connected or self.conn is None:
            print("[ERROR] Database not connected")
            return False
            
        try:
            # Aggression participants
            aggression_path = merged_dir / "aggression_participants.csv"
            if aggression_path.exists():
                df = pd.read_csv(aggression_path)
                print(f"[LOAD] Loading aggression participants: {len(df)} records")
                
                create_sql = """
                CREATE TABLE IF NOT EXISTS aggression_participants (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    participant_id TEXT NOT NULL,
                    gaming_hours_daily REAL,
                    age INTEGER,
                    gender TEXT,
                    aggression_score REAL,
                    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
                
                self.conn.execute(create_sql)
                df.to_sql('aggression_participants', self.conn, if_exists='replace', index=False)
                self.conn.commit()
                print(f"[SUCCESS] Aggression participants table created: {len(df)} records")
                self.tables_created.append(('aggression_participants', len(df)))
            
            # 7-scales participants
            scales_path = merged_dir / "7scales_participants.csv"
            if scales_path.exists():
                df = pd.read_csv(scales_path)
                print(f"[LOAD] Loading 7-scales participants: {len(df)} records")
                
                create_sql = """
                CREATE TABLE IF NOT EXISTS scales_participants (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    participant_id TEXT NOT NULL,
                    gaming_hours_daily REAL,
                    age INTEGER,
                    openness_score REAL,
                    conscientiousness_score REAL,
                    extraversion_score REAL,
                    agreeableness_score REAL,
                    neuroticism_score REAL,
                    gaming_addiction_score REAL,
                    social_gaming_score REAL,
                    gaming_genre_preference TEXT,
                    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
                
                self.conn.execute(create_sql)
                df.to_sql('scales_participants', self.conn, if_exists='replace', index=False)
                self.conn.commit()
                print(f"[SUCCESS] 7-scales participants table created: {len(df)} records")
                self.tables_created.append(('scales_participants', len(df)))
                
            return True
            
        except Exception as e:
            print(f"[ERROR] Failed to create participant tables: {e}")
            return False
    
    def create_indexes(self):
        """Create database indexes for performance"""
        if not self.connected or self.conn is None:
            print("[ERROR] Database not connected")
            return False
            
        try:
            print("[INDEX] Creating database indexes...")
            
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_comprehensive_participant ON comprehensive_gaming_data(participant_id)",
                "CREATE INDEX IF NOT EXISTS idx_comprehensive_gaming_hours ON comprehensive_gaming_data(gaming_hours_daily)",
                "CREATE INDEX IF NOT EXISTS idx_comprehensive_age ON comprehensive_gaming_data(age)",
                "CREATE INDEX IF NOT EXISTS idx_comprehensive_genre ON comprehensive_gaming_data(preferred_genre)",
                "CREATE INDEX IF NOT EXISTS idx_wellbeing_user ON gaming_wellbeing(user_id)",
                "CREATE INDEX IF NOT EXISTS idx_wellbeing_game ON gaming_wellbeing(game_title)",
                "CREATE INDEX IF NOT EXISTS idx_games_genre ON steam_games(genre)",
                "CREATE INDEX IF NOT EXISTS idx_games_year ON steam_games(release_year)"
            ]
            
            for index_sql in indexes:
                self.conn.execute(index_sql)
            
            self.conn.commit()
            print(f"[SUCCESS] Created {len(indexes)} database indexes")
            return True
            
        except Exception as e:
            print(f"[ERROR] Failed to create indexes: {e}")
            return False
    
    def create_analysis_views(self):
        """Create useful views for analysis"""
        if not self.connected or self.conn is None:
            print("[ERROR] Database not connected")
            return False
            
        try:
            print("[VIEWS] Creating analysis views...")
            
            # Gaming behavior analysis view
            gaming_analysis_view = """
            CREATE VIEW IF NOT EXISTS gaming_behavior_analysis AS
            SELECT 
                participant_id,
                gaming_hours_daily,
                age,
                gender,
                preferred_genre,
                anxiety_score,
                aggression_score,
                gaming_addiction_score,
                social_gaming_score,
                CASE 
                    WHEN gaming_hours_daily < 1 THEN 'Light'
                    WHEN gaming_hours_daily < 3 THEN 'Moderate' 
                    WHEN gaming_hours_daily < 6 THEN 'Heavy'
                    ELSE 'Extreme'
                END as gaming_intensity,
                CASE
                    WHEN age < 18 THEN 'Teen'
                    WHEN age < 25 THEN 'Young Adult'
                    WHEN age < 35 THEN 'Adult'
                    ELSE 'Mature'
                END as age_group
            FROM comprehensive_gaming_data
            WHERE participant_id IS NOT NULL
            """
            
            # Psychology scores analysis view
            psychology_analysis_view = """
            CREATE VIEW IF NOT EXISTS psychology_analysis AS
            SELECT 
                participant_id,
                openness_score,
                conscientiousness_score,
                extraversion_score,
                agreeableness_score,
                neuroticism_score,
                anxiety_score,
                aggression_score,
                gaming_addiction_score,
                gaming_hours_daily,
                (openness_score + conscientiousness_score + extraversion_score + 
                 agreeableness_score + (5 - neuroticism_score)) / 5 as big_five_wellness,
                CASE 
                    WHEN gaming_addiction_score > 3.5 THEN 'High Risk'
                    WHEN gaming_addiction_score > 2.5 THEN 'Moderate Risk'
                    ELSE 'Low Risk'
                END as addiction_risk_level
            FROM comprehensive_gaming_data
            WHERE openness_score IS NOT NULL
            """
            
            # Wellbeing summary view
            wellbeing_summary_view = """
            CREATE VIEW IF NOT EXISTS wellbeing_summary AS
            SELECT 
                game_title,
                COUNT(*) as player_count,
                AVG(hours_played) as avg_hours_played,
                AVG(wellbeing_score) as avg_wellbeing,
                AVG(stress_level) as avg_stress,
                AVG(social_connection_score) as avg_social_connection,
                AVG(achievement_satisfaction) as avg_achievement_satisfaction
            FROM gaming_wellbeing
            WHERE game_title IS NOT NULL
            GROUP BY game_title
            HAVING COUNT(*) >= 5
            ORDER BY avg_wellbeing DESC
            """
            
            views = [gaming_analysis_view, psychology_analysis_view, wellbeing_summary_view]
            
            for view_sql in views:
                self.conn.execute(view_sql)
            
            self.conn.commit()
            print(f"[SUCCESS] Created {len(views)} analysis views")
            return True
            
        except Exception as e:
            print(f"[ERROR] Failed to create views: {e}")
            return False
    
    def generate_database_summary(self):
        """Generate summary of created database"""
        if not self.connected or self.conn is None:
            print("[ERROR] Database not connected")
            return False
            
        try:
            print("\n" + "=" * 50)
            print("DATABASE CREATION SUMMARY")
            print("=" * 50)
            print(f"Author: Reannon Gray")
            print(f"Database: {self.db_path}")
            
            # Get database size
            db_size = Path(self.db_path).stat().st_size / (1024 * 1024)  # MB
            print(f"Size: {db_size:.2f} MB")
            
            print(f"\n[DATA] TABLES CREATED:")
            total_records = 0
            for table_name, record_count in self.tables_created:
                print(f"   {table_name:25}: {record_count:,} records")
                total_records += record_count
            
            print(f"\n[TOTAL] RECORDS: {total_records:,}")
            
            # Test key queries
            print(f"\n[SAMPLE] QUERIES:")
            
            # Gaming hours distribution
            cursor = self.conn.execute("""
                SELECT gaming_intensity, COUNT(*) as count 
                FROM gaming_behavior_analysis 
                GROUP BY gaming_intensity
                ORDER BY count DESC
            """)
            gaming_dist = cursor.fetchall()
            if gaming_dist:
                print(f"   Gaming Intensity Distribution:")
                for intensity, count in gaming_dist:
                    print(f"     {intensity}: {count} players")
            
            # Top genres
            cursor = self.conn.execute("""
                SELECT preferred_genre, COUNT(*) as count 
                FROM comprehensive_gaming_data 
                WHERE preferred_genre IS NOT NULL
                GROUP BY preferred_genre 
                ORDER BY count DESC LIMIT 5
            """)
            genres = cursor.fetchall()
            if genres:
                print(f"   Top Game Genres:")
                for genre, count in genres:
                    print(f"     {genre}: {count} players")
            
            print(f"\n[SUCCESS] Database creation completed successfully!")
            print(f"[TIP] Ready for analysis and visualization!")
            
            return True
            
        except Exception as e:
            print(f"[ERROR] Failed to generate summary: {e}")
            return False
    
    def close(self):
        """Close database connection"""
        if self.conn is not None:
            self.conn.close()
            self.connected = False
            print(f"[OK] Database connection closed")
        else:
            print(f"[INFO] No database connection to close")

def main():
    """Main database creation pipeline"""
    try:
        # Setup database path
        db_path = db_dir / "respawn_gaming_psychology.db"
        
        # Remove existing database for fresh start
        if db_path.exists():
            db_path.unlink()
            print(f"[INFO] Removed existing database")
        
        # Create database
        creator = GamingDatabaseCreator(db_path)
        
        if not creator.connect():
            return False
        
        print(f"\nCREATING DATABASE TABLES...")
        
        # Create all tables with explicit boolean handling
        results = []
        results.append(creator.create_comprehensive_table())
        results.append(creator.create_wellbeing_table())
        results.append(creator.create_games_table())
        results.append(creator.create_participant_tables())
        
        # Check if all table creation succeeded
        tables_success = all(result is True for result in results)
        
        if tables_success:
            index_result = creator.create_indexes()
            views_result = creator.create_analysis_views()
            
            if index_result is True and views_result is True:
                summary_result = creator.generate_database_summary()
                success = summary_result is True
            else:
                success = False
        else:
            success = False
        
        creator.close()
        return success
        
    except Exception as e:
        print(f"[ERROR] Database creation failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    if success:
        print(f"\n[READY] RespawnMetrics database ready for analysis!")
    else:
        print(f"\n[FAILED] Database creation failed!")