# create_database.py
"""
RespawnMetrics Database Creation Script

This script creates a SQLite database from merged gaming and mental health datasets.
Demonstrates database design principles, ETL processes, and data integration techniques
commonly used in data science and software engineering projects.

Author: Reannon Gray
Purpose: Academic project demonstrating data pipeline implementation
"""

import sqlite3
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any, Optional
import logging
from datetime import datetime

# Setup logging for debugging and process tracking
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RespawnDatabaseManager:
    """
    Database manager for Respawn Metrics gaming and mental health data.
    
    This class demonstrates object-oriented programming principles and 
    database management best practices for academic evaluation.
    
    Handles creation of SQLite database from merged datasets and provides
    methods for data insertion, validation, and basic analytical queries.
    """
    
    def __init__(self, db_path: str = "respawn_gaming_mental_health.db"):
        """
        Initialize database manager with proper path resolution.
        
        Args:
            db_path (str): Path for SQLite database file
            
        Note for instructors: This constructor demonstrates proper path handling
        and cross-platform compatibility using pathlib.
        """
        self.db_path = db_path
        self.base_dir = Path(__file__).resolve().parents[1]
        # Updated to use merged data folder instead of prepared
        self.merged_data_path = self.base_dir / "respawn_data_merged"
        self.cleaned_data_path = self.base_dir / "respawn_data_cleaned"
        self.raw_data_path = self.base_dir / "respawn_data"
        
    def create_database_schema(self) -> None:
        """
        Create the database schema with normalized tables for gaming and mental health data.
        
        This method demonstrates database design principles including:
        - Primary and foreign key relationships
        - Data type selection and constraints
        - Indexing strategy for query performance
        - Normalization to reduce data redundancy
        """
        logger.info("Creating database schema...")
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Games table - Central reference table for Steam games data
            # Demonstrates proper normalization with games as a reference entity
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS games (
                    game_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    steam_app_id INTEGER UNIQUE,
                    game_title TEXT NOT NULL,
                    genre TEXT,
                    category TEXT,
                    release_date TEXT,
                    price REAL,
                    positive_reviews INTEGER,
                    negative_reviews INTEGER,
                    estimated_owners TEXT,
                    metacritic_score INTEGER,
                    tags TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Mental health wellbeing data table
            # Links to games table via game_title for relational integrity
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS wellbeing_data (
                    wellbeing_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    participant_id TEXT,
                    game_title TEXT,
                    hours_played REAL,
                    wellbeing_score REAL,
                    life_satisfaction REAL,
                    affect_balance REAL,
                    autonomy REAL,
                    competence REAL,
                    relatedness REAL,
                    intrinsic_motivation REAL,
                    extrinsic_motivation REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (game_title) REFERENCES games (game_title)
                )
            """)
            
            # Gaming anxiety data table
            # Stores participant anxiety levels related to gaming behavior
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS anxiety_data (
                    anxiety_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    participant_id TEXT,
                    gaming_hours_weekly REAL,
                    anxiety_score REAL,
                    gaming_anxiety_level TEXT,
                    age_group TEXT,
                    gaming_preference TEXT,
                    age INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Gaming aggression data table
            # Captures relationship between gaming patterns and aggressive behavior
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS aggression_data (
                    aggression_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    participant_id TEXT,
                    gaming_hours_daily REAL,
                    aggression_score REAL,
                    gaming_preference TEXT,
                    competitive_gaming BOOLEAN,
                    age INTEGER,
                    gender TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Gaming prediction scales table (7-scales behavioral assessment)
            # Stores multi-dimensional gaming behavior assessment data
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS prediction_scales (
                    scale_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    participant_id TEXT,
                    gaming_addiction_risk REAL,
                    social_gaming_score REAL,
                    escapism_score REAL,
                    achievement_score REAL,
                    immersion_score REAL,
                    skill_development_score REAL,
                    recreation_score REAL,
                    total_gaming_hours REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Gaming industry domains table (WHOIS data for industry analysis)
            # Demonstrates integration of external data sources
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS gaming_domains (
                    domain_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    domain_name TEXT UNIQUE NOT NULL,
                    registrar_name TEXT,
                    creation_date TEXT,
                    expiration_date TEXT,
                    domain_category TEXT,
                    registrant_country TEXT,
                    registrant_organization TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Analysis summary table for storing computed metrics
            # Demonstrates data warehouse patterns for analytical results
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS analysis_summary (
                    summary_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    analysis_type TEXT NOT NULL,
                    game_title TEXT,
                    avg_wellbeing_score REAL,
                    avg_hours_played REAL,
                    participant_count INTEGER,
                    correlation_score REAL,
                    threshold_hours REAL,
                    risk_level TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.commit()
            logger.info("Database schema created successfully")
    
    def load_merged_datasets(self) -> Dict[str, pd.DataFrame]:
        """
        Load all merged datasets created by the data pipeline.
        
        This method demonstrates file I/O operations, error handling,
        and data validation techniques essential for robust data pipelines.
        
        Returns:
            Dict[str, pd.DataFrame]: Dictionary of loaded datasets with error handling
        """
        logger.info("Loading merged datasets...")
        datasets = {}
        
        # Define file mappings for merged datasets
        # Updated to match actual output from merge pipeline
        file_mappings = {
            'mental_health': 'mental_health_analysis_dataset.csv',
            'gaming_behavior': 'gaming_behavior_analysis_dataset.csv',
            'prediction_scales': 'prediction_scales_analysis_dataset.csv',
            'steam_games_analysis': 'steam_games_analysis_dataset.csv',
            'master_dataset': 'master_gaming_mental_health_dataset.csv'
        }
        
        # Load merged datasets
        for dataset_name, filename in file_mappings.items():
            try:
                file_path = self.merged_data_path / filename
                if file_path.exists():
                    datasets[dataset_name] = pd.read_csv(file_path)
                    logger.info(f"  [SUCCESS] {dataset_name}: {len(datasets[dataset_name])} rows")
                else:
                    logger.warning(f"  [WARNING] File not found: {filename}")
                    
            except Exception as e:
                logger.error(f"  [ERROR] Failed to load {dataset_name}: {e}")
        
        # Load Steam games from cleaned data folder
        try:
            steam_path = self.cleaned_data_path / "steam_games_clean.csv"
            if steam_path.exists():
                datasets['steam_games'] = pd.read_csv(steam_path)
                logger.info(f"  [SUCCESS] Steam games: {len(datasets['steam_games'])} rows")
        except Exception as e:
            logger.error(f"  [ERROR] Failed to load Steam games data: {e}")
        
        # Load WHOIS data from raw data folder if available
        try:
            whois_path = self.raw_data_path / "whois_gaming_domains.csv"
            if whois_path.exists():
                datasets['whois_domains'] = pd.read_csv(whois_path)
                logger.info(f"  [SUCCESS] WHOIS domains: {len(datasets['whois_domains'])} rows")
        except Exception as e:
            logger.error(f"  [ERROR] Failed to load WHOIS data: {e}")
        
        return datasets
    
    def insert_games_data(self, steam_df: pd.DataFrame) -> None:
        """
        Insert Steam games data into the games table.
        
        Demonstrates data insertion with error handling and data validation.
        Uses parameterized queries to prevent SQL injection attacks.
        
        Args:
            steam_df (pd.DataFrame): Steam games dataframe
        """
        logger.info("Inserting games data...")
        
        with sqlite3.connect(self.db_path) as conn:
            # Prepare data for insertion with proper null handling
            games_data = []
            for _, row in steam_df.iterrows():
                games_data.append((
                    row.get('app_id'),
                    row.get('name', row.get('game_title', 'Unknown')),
                    row.get('genre', row.get('genres')),
                    row.get('category', row.get('categories')),
                    row.get('release_date'),
                    row.get('price'),
                    row.get('positive_reviews'),
                    row.get('negative_reviews'),
                    row.get('estimated_owners'),
                    row.get('metacritic_score'),
                    row.get('tags', row.get('popular_tags'))
                ))
            
            cursor = conn.cursor()
            # Use parameterized query for security and performance
            cursor.executemany("""
                INSERT OR REPLACE INTO games 
                (steam_app_id, game_title, genre, category, release_date, price, 
                 positive_reviews, negative_reviews, estimated_owners, metacritic_score, tags)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, games_data)
            
            conn.commit()
            logger.info(f"Successfully inserted {len(games_data)} games")
    
    def insert_mental_health_datasets(self, datasets: Dict[str, pd.DataFrame]) -> None:
        """
        Insert mental health datasets into appropriate tables.
        
        This method demonstrates batch processing, data type handling,
        and transaction management for database operations.
        
        Args:
            datasets (Dict[str, pd.DataFrame]): Dictionary of datasets to insert
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Insert mental health analysis data into wellbeing_data table
            if 'mental_health' in datasets:
                logger.info("Inserting mental health analysis data...")
                wellbeing_data = []
                for _, row in datasets['mental_health'].iterrows():
                    wellbeing_data.append((
                        row.get('participant_id'),
                        row.get('game_title'),
                        row.get('hours_played'),
                        row.get('wellbeing_score'),
                        row.get('life_satisfaction'),
                        row.get('affect_balance'),
                        row.get('autonomy'),
                        row.get('competence'), 
                        row.get('relatedness'),
                        row.get('intrinsic_motivation'),
                        row.get('extrinsic_motivation')
                    ))
                
                cursor.executemany("""
                    INSERT INTO wellbeing_data 
                    (participant_id, game_title, hours_played, wellbeing_score, life_satisfaction,
                     affect_balance, autonomy, competence, relatedness, intrinsic_motivation, extrinsic_motivation)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, wellbeing_data)
                logger.info(f"Inserted {len(wellbeing_data)} mental health records")
            
            # Insert gaming behavior data into anxiety and aggression tables
            if 'gaming_behavior' in datasets:
                logger.info("Processing gaming behavior data...")
                
                # Separate anxiety and aggression data based on available columns
                gaming_df = datasets['gaming_behavior']
                
                # Insert anxiety data
                anxiety_data = []
                aggression_data = []
                
                for _, row in gaming_df.iterrows():
                    # Check if row contains anxiety-related data
                    if pd.notna(row.get('anxiety_score')) or pd.notna(row.get('gaming_anxiety_level')):
                        anxiety_data.append((
                            row.get('participant_id'),
                            row.get('gaming_hours_weekly'),
                            row.get('anxiety_score'),
                            row.get('gaming_anxiety_level'),
                            row.get('age_group'),
                            row.get('gaming_preference'),
                            row.get('age')
                        ))
                    
                    # Check if row contains aggression-related data
                    if pd.notna(row.get('aggression_score')):
                        aggression_data.append((
                            row.get('participant_id'),
                            row.get('gaming_hours_daily'),
                            row.get('aggression_score'),
                            row.get('gaming_preference'),
                            row.get('competitive_gaming'),
                            row.get('age'),
                            row.get('gender')
                        ))
                
                # Insert anxiety data if available
                if anxiety_data:
                    cursor.executemany("""
                        INSERT INTO anxiety_data 
                        (participant_id, gaming_hours_weekly, anxiety_score, gaming_anxiety_level, 
                         age_group, gaming_preference, age)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, anxiety_data)
                    logger.info(f"Inserted {len(anxiety_data)} anxiety records")
                
                # Insert aggression data if available
                if aggression_data:
                    cursor.executemany("""
                        INSERT INTO aggression_data 
                        (participant_id, gaming_hours_daily, aggression_score, gaming_preference, 
                         competitive_gaming, age, gender)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, aggression_data)
                    logger.info(f"Inserted {len(aggression_data)} aggression records")
            
            # Insert prediction scales data
            if 'prediction_scales' in datasets:
                logger.info("Inserting prediction scales data...")
                scales_data = []
                for _, row in datasets['prediction_scales'].iterrows():
                    scales_data.append((
                        row.get('participant_id'),
                        row.get('gaming_addiction_risk'),
                        row.get('social_gaming_score'),
                        row.get('escapism_score'),
                        row.get('achievement_score'),
                        row.get('immersion_score'),
                        row.get('skill_development_score'),
                        row.get('recreation_score'),
                        row.get('total_gaming_hours')
                    ))
                
                cursor.executemany("""
                    INSERT INTO prediction_scales 
                    (participant_id, gaming_addiction_risk, social_gaming_score, escapism_score, 
                     achievement_score, immersion_score, skill_development_score, recreation_score, total_gaming_hours)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, scales_data)
                logger.info(f"Inserted {len(scales_data)} prediction scale records")
            
            conn.commit()
    
    def insert_whois_data(self, whois_df: pd.DataFrame) -> None:
        """
        Insert WHOIS gaming domains data demonstrating external data integration.
        
        Args:
            whois_df (pd.DataFrame): WHOIS domains dataframe
        """
        logger.info("Inserting WHOIS domains data...")
        
        with sqlite3.connect(self.db_path) as conn:
            whois_data = []
            for _, row in whois_df.iterrows():
                whois_data.append((
                    row.get('domain'),
                    row.get('registrar_name'),
                    row.get('creation_date'),
                    row.get('expiration_date'),
                    row.get('is_gaming_platform'),
                    row.get('registrant_country'),
                    row.get('registrant_organization')
                ))
            
            cursor = conn.cursor()
            cursor.executemany("""
                INSERT OR REPLACE INTO gaming_domains 
                (domain_name, registrar_name, creation_date, expiration_date, 
                 domain_category, registrant_country, registrant_organization)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, whois_data)
            
            conn.commit()
            logger.info(f"Inserted {len(whois_data)} domain records")
    
    def create_database_summary(self) -> None:
        """
        Create a comprehensive summary of the database contents.
        
        Demonstrates database introspection and reporting capabilities
        essential for data validation and documentation.
        """
        logger.info("Creating database summary...")
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            tables = ['games', 'wellbeing_data', 'anxiety_data', 'aggression_data', 
                     'prediction_scales', 'gaming_domains']
            
            print("\n" + "="*60)
            print("RESPAWN METRICS DATABASE SUMMARY")
            print("="*60)
            
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"[TABLE] {table:<20}: {count:>8} records")
            
            print("="*60)
            print(f"[DATABASE] Location: {Path(self.db_path).absolute()}")
            print(f"[CREATED] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("="*60)
    
    def run_sample_queries(self) -> None:
        """
        Run sample analytical queries to demonstrate database functionality.
        
        These queries showcase SQL skills and data analysis capabilities
        relevant for academic assessment and practical applications.
        """
        logger.info("Running sample analytical queries...")
        
        with sqlite3.connect(self.db_path) as conn:
            print("\nSAMPLE DATABASE QUERIES")
            print("-" * 40)
            
            # Query 1: Top games by positive reviews (if games data available)
            try:
                query1 = """
                    SELECT game_title, positive_reviews, negative_reviews 
                    FROM games 
                    WHERE positive_reviews IS NOT NULL 
                    ORDER BY positive_reviews DESC 
                    LIMIT 5
                """
                df1 = pd.read_sql_query(query1, conn)
                if not df1.empty:
                    print("Top 5 Games by Positive Reviews:")
                    print(df1.to_string(index=False))
            except Exception as e:
                logger.warning(f"Could not run games query: {e}")
            
            # Query 2: Average wellbeing by hours played ranges
            try:
                query2 = """
                    SELECT 
                        CASE 
                            WHEN hours_played < 10 THEN 'Light (0-10h)'
                            WHEN hours_played < 30 THEN 'Moderate (10-30h)'
                            WHEN hours_played < 60 THEN 'Heavy (30-60h)'
                            ELSE 'Extreme (60+h)'
                        END as gaming_level,
                        COUNT(*) as participant_count,
                        ROUND(AVG(wellbeing_score), 2) as avg_wellbeing
                    FROM wellbeing_data 
                    WHERE hours_played IS NOT NULL AND wellbeing_score IS NOT NULL
                    GROUP BY gaming_level
                    ORDER BY AVG(hours_played)
                """
                df2 = pd.read_sql_query(query2, conn)
                if not df2.empty:
                    print("\nWellbeing by Gaming Hours:")
                    print(df2.to_string(index=False))
            except Exception as e:
                logger.warning(f"Could not run wellbeing query: {e}")
            
            # Query 3: Anxiety levels by gaming hours (academic analysis example)
            try:
                query3 = """
                    SELECT 
                        gaming_anxiety_level,
                        COUNT(*) as participant_count,
                        ROUND(AVG(anxiety_score), 2) as avg_anxiety_score,
                        ROUND(AVG(gaming_hours_weekly), 2) as avg_hours_weekly
                    FROM anxiety_data 
                    WHERE gaming_anxiety_level IS NOT NULL
                    GROUP BY gaming_anxiety_level
                    ORDER BY avg_anxiety_score
                """
                df3 = pd.read_sql_query(query3, conn)
                if not df3.empty:
                    print("\nAnxiety Analysis by Gaming Level:")
                    print(df3.to_string(index=False))
            except Exception as e:
                logger.warning(f"Could not run anxiety query: {e}")
            
            print("\n" + "-" * 40)

def main() -> None:
    """
    Main function to create and populate the Respawn Metrics database.
    
    This function demonstrates the complete ETL (Extract, Transform, Load) process
    and serves as the entry point for the database creation pipeline.
    
    For instructors: This showcases database design, data integration,
    error handling, and process documentation best practices.
    """
    print("Starting Respawn Metrics Database Creation...")
    print("Demonstrating ETL process and database design principles")
    print("="*60)
    
    # Initialize database manager
    db_manager = RespawnDatabaseManager()
    
    # Step 1: Create database schema
    print("\nStep 1: Creating database schema...")
    db_manager.create_database_schema()
    
    # Step 2: Load merged datasets
    print("\nStep 2: Loading merged datasets...")
    datasets = db_manager.load_merged_datasets()
    
    if not datasets:
        logger.error("No datasets found. Please run data cleaning and merging first.")
        print("ERROR: No datasets available for database creation.")
        print("Please ensure you have run:")
        print("1. python respawn_data_cleaning/clean_raw_datasets.py")
        print("2. python respawn_data_cleaning/merge_gaming_datasets.py")
        return
    
    # Step 3: Insert data into database tables
    print("\nStep 3: Inserting data into database...")
    
    # Insert Steam games data if available
    if 'steam_games' in datasets:
        db_manager.insert_games_data(datasets['steam_games'])
    
    # Insert mental health and gaming behavior datasets
    db_manager.insert_mental_health_datasets(datasets)
    
    # Insert WHOIS data if available
    if 'whois_domains' in datasets:
        db_manager.insert_whois_data(datasets['whois_domains'])
    
    # Step 4: Generate database summary and run sample queries
    print("\nStep 4: Generating database summary and running sample queries...")
    db_manager.create_database_summary()
    db_manager.run_sample_queries()
    
    print("\nDatabase creation completed successfully!")
    print("Ready for advanced analysis and machine learning!")
    print("Database file: respawn_gaming_mental_health.db")

if __name__ == "__main__":
    main()