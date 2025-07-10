import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
from datetime import datetime
import csv
from urllib.parse import urljoin, urlparse
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AlibabaRFQScraper:
    def __init__(self):
        self.base_url = "https://sourcing.alibaba.com"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        
    def get_page(self, url, retries=3):
        """Fetch a page with retry logic"""
        for attempt in range(retries):
            try:
                response = self.session.get(url, timeout=10)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    raise
    
    def parse_time_ago(self, time_str):
        """Parse relative time strings like '1 Hour before', '12 days ago'"""
        if not time_str:
            return ""
        
        time_str = time_str.lower().strip()
        
        # Map of time units to days
        time_mapping = {
            'hour': 1/24,
            'hours': 1/24,
            'day': 1,
            'days': 1,
            'week': 7,
            'weeks': 7,
            'month': 30,
            'months': 30
        }
        
        # Extract number and unit
        match = re.search(r'(\d+)\s*(hour|hours|day|days|week|weeks|month|months)', time_str)
        if match:
            number = int(match.group(1))
            unit = match.group(2)
            
            if unit in time_mapping:
                days_ago = number * time_mapping[unit]
                target_date = datetime.now() - pd.Timedelta(days=days_ago)
                return target_date.strftime('%d-%m-%Y')
        
        return time_str
    
    def extract_rfq_data(self, rfq_element):
        """Extract RFQ data from a single RFQ element"""
        data = {
            'RFQ ID': '',
            'Title': '',
            'Buyer Name': '',
            'Buyer Image': '',
            'Inquiry Time': '',
            'Quotes Left': '',
            'Country': '',
            'Quantity Required': '',
            'Email Confirmed': 'No',
            'Experienced Buyer': 'No',
            'Complete Order via RFQ': 'No',
            'Typical Replies': 'No',
            'Interactive User': 'No',
            'Inquiry URL': '',
            'Inquiry Date': '',
            'Scraping Date': datetime.now().strftime('%d-%m-%Y')
        }
        
        try:
            # Extract RFQ ID from URL or data attributes
            link_elem = rfq_element.find('a', href=True)
            if link_elem:
                href = link_elem['href']
                data['Inquiry URL'] = urljoin(self.base_url, href)
                
                # Extract RFQ ID from URL parameters
                if 'ID' in href:
                    match = re.search(r'ID([^&]+)', href)
                    if match:
                        data['RFQ ID'] = match.group(1)
            
            # Extract title
            title_elem = rfq_element.find(['h3', 'h4', 'span'], class_=re.compile(r'title|subject'))
            if not title_elem:
                title_elem = rfq_element.find('a')
            if title_elem:
                data['Title'] = title_elem.get_text(strip=True)
            
            # Extract buyer information
            buyer_elem = rfq_element.find(['span', 'div'], class_=re.compile(r'buyer|user|name'))
            if buyer_elem:
                data['Buyer Name'] = buyer_elem.get_text(strip=True)
            
            # Extract buyer image
            img_elem = rfq_element.find('img')
            if img_elem and img_elem.get('src'):
                data['Buyer Image'] = img_elem['src']
            
            # Extract time information
            time_elem = rfq_element.find(['span', 'div'], string=re.compile(r'ago|before|hour|day|week|month'))
            if time_elem:
                time_text = time_elem.get_text(strip=True)
                data['Inquiry Time'] = time_text
                data['Inquiry Date'] = self.parse_time_ago(time_text)
            
            # Extract quotes left
            quotes_elem = rfq_element.find(['span', 'div'], string=re.compile(r'quote|left'))
            if quotes_elem:
                quotes_text = quotes_elem.get_text(strip=True)
                match = re.search(r'(\d+)', quotes_text)
                if match:
                    data['Quotes Left'] = match.group(1)
            
            # Extract country (look for flag images or country text)
            country_elem = rfq_element.find(['span', 'div'], class_=re.compile(r'country|flag'))
            if country_elem:
                data['Country'] = country_elem.get_text(strip=True)
            
            # Extract quantity
            quantity_elem = rfq_element.find(['span', 'div'], string=re.compile(r'piece|pcs|unit|box|kg|ton'))
            if quantity_elem:
                quantity_text = quantity_elem.get_text(strip=True)
                data['Quantity Required'] = quantity_text
            
            # Extract various flags (Email Confirmed, Experienced Buyer, etc.)
            # These are typically represented as icons or badges
            if rfq_element.find(['span', 'div', 'i'], class_=re.compile(r'email|confirm|verified')):
                data['Email Confirmed'] = 'Yes'
            
            if rfq_element.find(['span', 'div', 'i'], class_=re.compile(r'experienced|veteran|star')):
                data['Experienced Buyer'] = 'Yes'
            
            if rfq_element.find(['span', 'div', 'i'], class_=re.compile(r'complete|order|rfq')):
                data['Complete Order via RFQ'] = 'Yes'
            
            if rfq_element.find(['span', 'div', 'i'], class_=re.compile(r'typical|reply|response')):
                data['Typical Replies'] = 'Yes'
            
            if rfq_element.find(['span', 'div', 'i'], class_=re.compile(r'interactive|active|online')):
                data['Interactive User'] = 'Yes'
                
        except Exception as e:
            logger.error(f"Error extracting RFQ data: {e}")
        
        return data
    
    def scrape_rfq_page(self, url):
        """Scrape RFQ data from a single page"""
        logger.info(f"Scraping page: {url}")
        
        try:
            response = self.get_page(url)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find RFQ containers (these selectors may need adjustment based on actual HTML structure)
            rfq_containers = soup.find_all(['div', 'li'], class_=re.compile(r'rfq|item|card|inquiry|request'))
            
            # If no specific containers found, try broader search
            if not rfq_containers:
                rfq_containers = soup.find_all(['div', 'li'], class_=re.compile(r'list|item|card'))
            
            rfq_data = []
            
            for container in rfq_containers:
                # Skip if this doesn't look like an RFQ item
                if not container.find('a', href=True):
                    continue
                
                data = self.extract_rfq_data(container)
                if data['Title']:  # Only add if we found a title
                    rfq_data.append(data)
            
            return rfq_data
            
        except Exception as e:
            logger.error(f"Error scraping page {url}: {e}")
            return []
    
    def get_all_page_urls(self, base_url):
        """Get all page URLs for pagination"""
        urls = [base_url]
        
        try:
            response = self.get_page(base_url)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find pagination links
            pagination = soup.find(['div', 'ul'], class_=re.compile(r'pag|page'))
            
            if pagination:
                page_links = pagination.find_all('a', href=True)
                for link in page_links:
                    href = link['href']
                    if 'page' in href.lower() or re.search(r'p=\d+', href):
                        full_url = urljoin(self.base_url, href)
                        if full_url not in urls:
                            urls.append(full_url)
            
            # Also try to find "Next" buttons and follow them
            next_link = soup.find('a', string=re.compile(r'next|more|»|>', re.I))
            if next_link and next_link.get('href'):
                next_url = urljoin(self.base_url, next_link['href'])
                if next_url not in urls:
                    urls.append(next_url)
            
        except Exception as e:
            logger.error(f"Error getting pagination URLs: {e}")
        
        return urls
    
    def scrape_all_pages(self, start_url, max_pages=50):
        """Scrape all pages starting from the given URL"""
        all_data = []
        
        # Get all page URLs
        page_urls = self.get_all_page_urls(start_url)
        
        # Limit the number of pages to avoid infinite scraping
        page_urls = page_urls[:max_pages]
        
        logger.info(f"Found {len(page_urls)} pages to scrape")
        
        for i, url in enumerate(page_urls, 1):
            logger.info(f"Scraping page {i}/{len(page_urls)}")
            
            page_data = self.scrape_rfq_page(url)
            all_data.extend(page_data)
            
            # Add delay between requests to be respectful
            time.sleep(2)
            
            # Break if we haven't found any data on this page
            if not page_data:
                logger.warning(f"No data found on page {i}, stopping")
                break
        
        return all_data
    
    def save_to_csv(self, data, filename='alibaba_rfq_data.csv'):
        """Save scraped data to CSV file"""
        if not data:
            logger.warning("No data to save")
            return
        
        df = pd.DataFrame(data)
        
        # Ensure all required columns are present
        required_columns = [
            'RFQ ID', 'Title', 'Buyer Name', 'Buyer Image', 'Inquiry Time',
            'Quotes Left', 'Country', 'Quantity Required', 'Email Confirmed',
            'Experienced Buyer', 'Complete Order via RFQ', 'Typical Replies',
            'Interactive User', 'Inquiry URL', 'Inquiry Date', 'Scraping Date'
        ]
        
        for col in required_columns:
            if col not in df.columns:
                df[col] = ''
        
        # Reorder columns to match template
        df = df[required_columns]
        
        # Save to CSV
        df.to_csv(filename, index=False, encoding='utf-8')
        logger.info(f"Data saved to {filename}")
        logger.info(f"Total records: {len(df)}")
        
        return df

def main():
    """Main function to run the scraper"""
    scraper = AlibabaRFQScraper()
    
    # URL to scrape
    start_url = "https://sourcing.alibaba.com/rfq/rfq_search_list.htm?spm=a2700.8073608.1998677541.1.82be65aaoUUItC&country=AE&recently=Y&tracelog=newest"
    
    try:
        # Scrape all pages
        logger.info("Starting Alibaba RFQ scraping...")
        all_data = scraper.scrape_all_pages(start_url, max_pages=10)  # Limit to 10 pages for testing
        
        # Save to CSV
        filename = f"alibaba_rfq_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.csv"
        df = scraper.save_to_csv(all_data, filename)
        
        if df is not None:
            print(f"\nScraping completed successfully!")
            print(f"Total RFQs scraped: {len(df)}")
            print(f"Data saved to: {filename}")
            
            # Display first few rows
            print("\nFirst 5 rows of scraped data:")
            print(df.head().to_string(index=False))
        
    except Exception as e:
        logger.error(f"Scraping failed: {e}")
        print(f"Error: {e}")

if __name__ == "__main__":
    main()