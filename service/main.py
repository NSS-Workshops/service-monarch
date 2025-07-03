"""Main entry point for the Monarch service"""
import sys
import os

# Add the parent directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import asyncio
from service.core.monarch import TicketMigrator

def main():
    """Main entry point for the Monarch service"""
    migrator = TicketMigrator()
    asyncio.run(migrator.run())

if __name__ == "__main__":
    main()