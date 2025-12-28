"""
Download MovieLens dataset from the internet.
Task 1: Download the ml-32m.zip file
"""

import os
import sys
import requests
import zipfile
import logging
from datetime import datetime

# Add parent directory to path so we can import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import MOVIELENS_URL, ZIP_FILENAME, DATA_RAW_PATH, LOGS_PATH

# Setup logging
os.makedirs(LOGS_PATH, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'{LOGS_PATH}/pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def download_file(url, destination):
    """
    Download a file from a URL to a destination folder.
    
    Args:
        url: The URL to download from
        destination: The folder to save the file to
    
    Returns:
        Path to the downloaded file
    """
    try:
        logger.info(f"Starting download from {url}")
        
        # Create destination folder if it doesn't exist
        os.makedirs(destination, exist_ok=True)
        
        # Get the filename from URL
        filename = os.path.join(destination, ZIP_FILENAME)
        
        # Download with progress
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise error if download failed
        
        total_size = int(response.headers.get('content-length', 0))
        logger.info(f"File size: {total_size / (1024*1024):.2f} MB")
        
        # Write file in chunks
        with open(filename, 'wb') as f:
            downloaded = 0
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    # Log progress every 10%
                    if total_size > 0:
                        percent = (downloaded / total_size) * 100
                        if downloaded % (total_size // 10) < 8192:
                            logger.info(f"Downloaded: {percent:.1f}%")
        
        logger.info(f"Download complete: {filename}")
        return filename
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Download failed: {e}")
        raise


def extract_zip(zip_path, destination):
    """
    Extract a zip file to a destination folder.
    
    Args:
        zip_path: Path to the zip file
        destination: Folder to extract to
    
    Returns:
        List of extracted files
    """
    try:
        logger.info(f"Extracting {zip_path}")
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(destination)
            extracted_files = zip_ref.namelist()
        
        logger.info(f"Extracted {len(extracted_files)} files")
        return extracted_files
        
    except zipfile.BadZipFile as e:
        logger.error(f"Extraction failed: {e}")
        raise


def main():
    """Main function to download and extract MovieLens data."""
    logger.info("=" * 50)
    logger.info("TASK 1: Download MovieLens Dataset")
    logger.info("=" * 50)
    
    start_time = datetime.now()
    
    # Step 1: Download the zip file
    zip_path = download_file(MOVIELENS_URL, DATA_RAW_PATH)
    
    # Step 2: Extract the zip file
    extracted_files = extract_zip(zip_path, DATA_RAW_PATH)
    
    # Log the extracted files
    logger.info("Extracted files:")
    for f in extracted_files[:10]:  # Show first 10 files
        logger.info(f"  - {f}")
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logger.info(f"Task 1 completed in {duration:.2f} seconds")
    
    return zip_path, extracted_files


if __name__ == "__main__":
    main()