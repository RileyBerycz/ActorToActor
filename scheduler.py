#!/usr/bin/env python3
"""
Simple scheduler for periodic actor database updates
Runs database updates at specified intervals without requiring GitHub Actions
"""

import os
import time
import schedule
import subprocess
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseScheduler:
    def __init__(self):
        self.tmdb_api_key = os.environ.get("TMDB_API_KEY")
        if not self.tmdb_api_key:
            logger.error("TMDB_API_KEY environment variable not set")
            raise ValueError("TMDB_API_KEY required")
    
    def run_database_update(self, pages=10):
        """Run the actor database update"""
        try:
            logger.info(f"Starting database update with {pages} pages...")
            
            # Run the actor service update command
            result = subprocess.run([
                'python', 'actor_service.py', 'update', str(pages)
            ], capture_output=True, text=True, timeout=3600)  # 1 hour timeout
            
            if result.returncode == 0:
                logger.info("Database update completed successfully")
                logger.info(f"Output: {result.stdout}")
            else:
                logger.error(f"Database update failed with return code {result.returncode}")
                logger.error(f"Error: {result.stderr}")
                
        except subprocess.TimeoutExpired:
            logger.error("Database update timed out after 1 hour")
        except Exception as e:
            logger.error(f"Error running database update: {e}")
    
    def daily_update(self):
        """Daily update with more pages"""
        logger.info("Running daily database update...")
        self.run_database_update(pages=50)
    
    def hourly_update(self):
        """Hourly update with fewer pages for recent changes"""
        logger.info("Running hourly database update...")
        self.run_database_update(pages=5)
    
    def startup_update(self):
        """Initial update on startup"""
        logger.info("Running startup database update...")
        self.run_database_update(pages=20)

def main():
    """Main scheduler loop"""
    scheduler = DatabaseScheduler()
    
    # Schedule updates
    schedule.every().day.at("02:00").do(scheduler.daily_update)  # 2 AM daily
    schedule.every().hour.at(":00").do(scheduler.hourly_update)  # Every hour
    
    # Run initial update
    scheduler.startup_update()
    
    logger.info("Scheduler started. Daily updates at 2 AM, hourly updates on the hour.")
    logger.info("Press Ctrl+C to stop.")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user")
    except Exception as e:
        logger.error(f"Scheduler error: {e}")

if __name__ == "__main__":
    main()
