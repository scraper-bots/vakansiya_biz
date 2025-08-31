import asyncio
import aiohttp
import json
import time
from bs4 import BeautifulSoup
import pandas as pd
from typing import Dict, List, Optional
import logging
import aiofiles
# from urllib.parse import urljoin  # Not needed
import re

class ComprehensiveVakansiyaScraper:
    def __init__(self, max_concurrent=10):
        self.base_api_url = "https://api.vakansiya.biz/api/v1/resumes/search"
        self.base_page_url = "https://vakansiya.biz/az/cv"
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.setup_logging()
        
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def get_headers(self):
        """Get headers for requests"""
        return {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,ru;q=0.7,az;q=0.6',
            'cache-control': 'no-cache',
            'pragma': 'no-cache',
            'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36'
        }
        
    def get_api_headers(self):
        """Get headers for API requests"""
        return {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,ru;q=0.7,az;q=0.6',
            'authorization': 'Bearer null',
            'locale': 'az',
            'origin': 'https://vakansiya.biz',
            'referer': 'https://vakansiya.biz/',
            'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
            'x-requested-with': 'XMLHttpRequest'
        }

    async def get_candidates_from_api(self, session: aiohttp.ClientSession, page: int = 1) -> Optional[Dict]:
        """Fetch candidates from the general listing API"""
        params = {
            'page': page,
            'country_id': 0,
            'city_id': 0,
            'industry_id': 0,
            'min_age': 0,
            'max_age': 0,
            'gender': -1,
            'title': ''
        }
        
        async with self.semaphore:
            try:
                headers = self.get_api_headers()
                async with session.get(self.base_api_url, params=params, headers=headers) as response:
                    response.raise_for_status()
                    return await response.json()
            except aiohttp.ClientError as e:
                self.logger.error(f"Error fetching API page {page}: {e}")
                return None

    async def get_all_candidates_basic(self) -> List[Dict]:
        """Get all candidates from the API with basic info"""
        connector = aiohttp.TCPConnector(limit=50, limit_per_host=10)
        timeout = aiohttp.ClientTimeout(total=30)
        
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout
        ) as session:
            all_candidates = []
            
            # Get first page to determine total pages
            first_page = await self.get_candidates_from_api(session, 1)
            if not first_page:
                return []
                
            all_candidates.extend(first_page['data'])
            last_page = first_page.get('last_page', 1)
            
            if last_page > 1:
                # Process remaining pages in smaller batches to avoid rate limiting
                batch_size = 5
                for start_page in range(2, last_page + 1, batch_size):
                    end_page = min(start_page + batch_size, last_page + 1)
                    tasks = []
                    
                    for page in range(start_page, end_page):
                        task = self.get_candidates_from_api(session, page)
                        tasks.append(task)
                    
                    self.logger.info(f"Fetching API pages {start_page}-{end_page-1}...")
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    for result in results:
                        if isinstance(result, dict) and result.get('data'):
                            all_candidates.extend(result['data'])
                        elif isinstance(result, Exception):
                            self.logger.error(f"API request failed: {result}")
                    
                    # Delay between batches
                    await asyncio.sleep(1)
                        
        self.logger.info(f"Total candidates fetched from API: {len(all_candidates)}")
        return all_candidates

    async def scrape_candidate_details(self, session: aiohttp.ClientSession, candidate_id: int, slug: str) -> Dict:
        """Scrape comprehensive detailed information from individual candidate page"""
        url = f"{self.base_page_url}/{candidate_id}/{slug}"
        
        async with self.semaphore:
            try:
                await asyncio.sleep(0.2)  # Small delay to be respectful
                
                headers = self.get_headers()
                async with session.get(url, headers=headers) as response:
                    response.raise_for_status()
                    html = await response.text()
                    
                soup = BeautifulSoup(html, 'html.parser')
                
                details = {
                    'candidate_id': candidate_id,
                    'slug': slug,
                    'url': url,
                    'summary': self.extract_summary(soup),
                    'contact_info': self.extract_contact_info(soup),
                    'experience': self.extract_experience(soup),
                    'education': self.extract_education(soup),
                    'awards_certificates': self.extract_awards_certificates(soup),
                    'skills': self.extract_skills(soup),
                    'languages': self.extract_languages(soup)
                }
                
                return details
                
            except aiohttp.ClientError as e:
                self.logger.error(f"Error scraping candidate {candidate_id}: {e}")
                return {'candidate_id': candidate_id, 'slug': slug, 'error': str(e), 'url': url}
            except Exception as e:
                self.logger.error(f"Unexpected error scraping candidate {candidate_id}: {e}")
                return {'candidate_id': candidate_id, 'slug': slug, 'error': str(e), 'url': url}

    def extract_summary(self, soup: BeautifulSoup) -> str:
        """Extract summary/headline information"""
        summary_section = soup.find('div', {'id': 'resume_headline_bx'})
        if summary_section:
            summary_p = summary_section.find('p')
            if summary_p:
                return summary_p.get_text(strip=True)
        return ""

    def extract_contact_info(self, soup: BeautifulSoup) -> Dict:
        """Extract detailed contact information"""
        contact_info = {}
        
        contacts_section = soup.find('div', {'id': 'contacts'})
        if contacts_section:
            # Extract all labeled contact fields
            for clearfix_div in contacts_section.find_all('div', class_='clearfix'):
                label = clearfix_div.find('label')
                span = clearfix_div.find('span', class_='clearfix')
                
                if label and span:
                    field_name = label.get_text(strip=True)
                    field_value = span.get_text(strip=True)
                    
                    if field_value and field_value != '---':
                        # Map Azerbaijani field names to English
                        field_mapping = {
                            'Ünvan': 'address',
                            'Yaş': 'age', 
                            'E-mail': 'email',
                            'Telefon nömrəsi': 'phone',
                            'Ailə vəziyyəti': 'marital_status',
                            'Linkedin': 'linkedin',
                            'Github': 'github',
                            'Skype': 'skype'
                        }
                        
                        english_field = field_mapping.get(field_name, field_name.lower())
                        contact_info[english_field] = field_value
        
        return contact_info

    def extract_experience(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract detailed work experience"""
        experiences = []
        
        employment_section = soup.find('div', {'id': 'employment_bx'})
        if employment_section:
            # Find all experience entries
            for exp_div in employment_section.find_all('div', class_='mb-3 ex_ed_aw'):
                experience = {}
                
                # Job title
                title_h5 = exp_div.find('h5', class_='font-16')
                if title_h5:
                    experience['job_title'] = title_h5.get_text(strip=True)
                
                # Company and location, dates, description
                paragraphs = exp_div.find_all('p', class_='m-b0')
                
                if len(paragraphs) >= 1:
                    company_info = paragraphs[0].get_text(strip=True)
                    if ', ' in company_info:
                        parts = company_info.split(', ')
                        experience['company'] = parts[0]
                        if len(parts) > 1:
                            experience['location'] = parts[1]
                    else:
                        experience['company'] = company_info
                
                if len(paragraphs) >= 2:
                    date_info = paragraphs[1].get_text(strip=True)
                    # Parse date range
                    if ' - ' in date_info:
                        dates = date_info.split(' - ')
                        experience['start_date'] = dates[0].strip()
                        if len(dates) > 1:
                            end_date = dates[1].strip()
                            # Remove any span tags
                            end_date = re.sub(r'<[^>]+>', '', end_date)
                            experience['end_date'] = end_date
                    else:
                        experience['dates'] = date_info
                
                if len(paragraphs) >= 3:
                    experience['description'] = paragraphs[2].get_text(strip=True)
                
                experiences.append(experience)
        
        return experiences

    def extract_education(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract detailed education information"""
        education_list = []
        
        education_section = soup.find('div', {'id': 'education_bx'})
        if education_section:
            # Find all education entries
            for edu_div in education_section.find_all('div', class_='mb-3 ex_ed_aw'):
                education = {}
                
                # Degree/Program title
                title_h5 = edu_div.find('h5', class_='font-16')
                if title_h5:
                    education['program'] = title_h5.get_text(strip=True)
                
                # Institution, location, degree level, dates
                paragraphs = edu_div.find_all('p', class_='m-b0')
                
                if len(paragraphs) >= 1:
                    institution_info = paragraphs[0].get_text(strip=True)
                    if ', ' in institution_info:
                        parts = institution_info.split(', ')
                        education['institution'] = parts[0]
                        if len(parts) > 1:
                            education['location'] = parts[1]
                    else:
                        education['institution'] = institution_info
                
                if len(paragraphs) >= 2:
                    degree_level = paragraphs[1].get_text(strip=True)
                    education['degree_level'] = degree_level
                
                if len(paragraphs) >= 3:
                    date_info = paragraphs[2].get_text(strip=True)
                    # Parse date range
                    if ' - ' in date_info:
                        dates = date_info.split(' - ')
                        education['start_date'] = dates[0].strip()
                        if len(dates) > 1:
                            end_date = dates[1].strip()
                            end_date = re.sub(r'<[^>]+>', '', end_date)
                            education['end_date'] = end_date
                    else:
                        education['dates'] = date_info
                
                education_list.append(education)
        
        return education_list

    def extract_awards_certificates(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract awards and certificates"""
        awards_list = []
        
        awards_section = soup.find('div', {'id': 'awards_bx'})
        if awards_section:
            for award_div in awards_section.find_all('div', class_='mb-3 ex_ed_aw'):
                award = {}
                
                # Award title
                title_h5 = award_div.find('h5', class_='font-16')
                if title_h5:
                    award['title'] = title_h5.get_text(strip=True)
                
                # Issuer, description, dates
                paragraphs = award_div.find_all('p', class_='m-b0')
                
                if len(paragraphs) >= 1:
                    award['issuer'] = paragraphs[0].get_text(strip=True)
                
                if len(paragraphs) >= 2:
                    award['description'] = paragraphs[1].get_text(strip=True)
                
                if len(paragraphs) >= 3:
                    date_info = paragraphs[2].get_text(strip=True)
                    if ' - ' in date_info:
                        dates = date_info.split(' - ')
                        award['start_date'] = dates[0].strip()
                        if len(dates) > 1:
                            end_date = dates[1].strip()
                            end_date = re.sub(r'<[^>]+>', '', end_date)
                            award['end_date'] = end_date
                    else:
                        award['dates'] = date_info
                
                awards_list.append(award)
        
        return awards_list

    def extract_skills(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract skills from the skills table"""
        skills_list = []
        
        skills_section = soup.find('div', {'id': 'it_skills_bx'})
        if skills_section:
            table = skills_section.find('table')
            if table:
                tbody = table.find('tbody')
                if tbody:
                    for row in tbody.find_all('tr'):
                        cells = row.find_all('td')
                        if len(cells) >= 3:
                            skill = {
                                'skill_name': cells[0].get_text(strip=True),
                                'proficiency_level': cells[1].get_text(strip=True),
                                'experience_years': cells[2].get_text(strip=True)
                            }
                            skills_list.append(skill)
        
        return skills_list

    def extract_languages(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract language skills from the languages table"""
        languages_list = []
        
        lang_section = soup.find('div', {'id': 'lang_skills_bx'})
        if lang_section:
            table = lang_section.find('table')
            if table:
                tbody = table.find('tbody')
                if tbody:
                    for row in tbody.find_all('tr'):
                        cells = row.find_all('td')
                        if len(cells) >= 2:
                            language = {
                                'language': cells[0].get_text(strip=True),
                                'proficiency_level': cells[1].get_text(strip=True)
                            }
                            languages_list.append(language)
        
        return languages_list

    async def scrape_all_candidates(self, limit: Optional[int] = None) -> List[Dict]:
        """Scrape all candidates with comprehensive detailed information"""
        start_time = time.time()
        
        # Get basic info from API
        basic_candidates = await self.get_all_candidates_basic()
        
        if limit:
            basic_candidates = basic_candidates[:limit]
            
        self.logger.info(f"Starting comprehensive scraping for {len(basic_candidates)} candidates...")
        
        connector = aiohttp.TCPConnector(limit=30, limit_per_host=10)
        timeout = aiohttp.ClientTimeout(total=30)
        
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout
        ) as session:
            
            # Create tasks for detailed scraping
            tasks = []
            for candidate in basic_candidates:
                if candidate.get('id') and candidate.get('slug'):
                    task = self.scrape_candidate_details(
                        session,
                        candidate.get('id'), 
                        candidate.get('slug')
                    )
                    tasks.append((candidate, task))
            
            # Execute scraping tasks in small batches
            detailed_candidates = []
            batch_size = 5  # Smaller batch size for detailed scraping
            
            for i in range(0, len(tasks), batch_size):
                batch = tasks[i:i + batch_size]
                self.logger.info(f"Processing comprehensive batch {i//batch_size + 1}/{(len(tasks) + batch_size - 1)//batch_size}")
                
                batch_results = await asyncio.gather(*[task for _, task in batch], return_exceptions=True)
                
                for j, result in enumerate(batch_results):
                    candidate = batch[j][0]
                    if isinstance(result, dict):
                        # Combine basic and detailed info
                        combined = {**candidate, **result}
                        detailed_candidates.append(combined)
                    else:
                        self.logger.error(f"Failed to scrape candidate {candidate.get('slug')}: {result}")
                        detailed_candidates.append({**candidate, 'scraping_error': str(result)})
                
                # Longer delay between batches for comprehensive scraping
                await asyncio.sleep(2)
                
        elapsed_time = time.time() - start_time
        self.logger.info(f"Comprehensive scraping completed in {elapsed_time:.2f} seconds")
        self.logger.info(f"Average time per candidate: {elapsed_time/len(detailed_candidates):.2f} seconds")
        
        return detailed_candidates

    async def save_to_json(self, candidates: List[Dict], filename: str = 'candidates.json'):
        """Save candidates data to JSON file"""
        async with aiofiles.open(filename, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(candidates, ensure_ascii=False, indent=2))
        self.logger.info(f"Saved {len(candidates)} comprehensive candidates to {filename}")

    def save_to_csv(self, candidates: List[Dict], filename: str = 'candidates.csv'):
        """Save candidates data to CSV file with flattened structure"""
        if not candidates:
            self.logger.warning("No candidates data to save")
            return
            
        flattened_candidates = []
        
        for candidate in candidates:
            # Basic info
            flattened = {
                'id': candidate.get('id'),
                'user_id': candidate.get('user_id'),
                'slug': candidate.get('slug'),
                'first_name': candidate.get('firstname'),
                'last_name': candidate.get('lastname'),
                'age': candidate.get('age'),
                'job_title': candidate.get('title'),
                'industry_title_az': candidate.get('industry', {}).get('title_az', ''),
                'industry_title_en': candidate.get('industry', {}).get('title_en', ''),
                'city': candidate.get('city', {}).get('title_en', ''),
                'country': candidate.get('country', {}).get('title_en', ''),
                'expected_salary': candidate.get('expected_salary'),
                'is_premium': candidate.get('is_premium'),
                'url': candidate.get('url'),
                
                # Summary
                'summary': candidate.get('summary', ''),
                
                # Contact info
                'address': candidate.get('contact_info', {}).get('address', ''),
                'email': candidate.get('contact_info', {}).get('email', ''),
                'phone': candidate.get('contact_info', {}).get('phone', ''),
                'marital_status': candidate.get('contact_info', {}).get('marital_status', ''),
                'linkedin': candidate.get('contact_info', {}).get('linkedin', ''),
                'github': candidate.get('contact_info', {}).get('github', ''),
                'skype': candidate.get('contact_info', {}).get('skype', ''),
                
                # Counts
                'experience_count': len(candidate.get('experience', [])),
                'education_count': len(candidate.get('education', [])),
                'awards_count': len(candidate.get('awards_certificates', [])),
                'skills_count': len(candidate.get('skills', [])),
                'languages_count': len(candidate.get('languages', [])),
                
                # Latest/Primary Experience
                'latest_job_title': '',
                'latest_company': '',
                'latest_job_description': '',
                
                # Latest Education
                'latest_degree': '',
                'latest_institution': '',
                
                # Skills summary
                'skills_summary': '',
                'languages_summary': '',
                
                # Error flag
                'has_error': 'error' in candidate or 'scraping_error' in candidate
            }
            
            # Add latest experience info
            if candidate.get('experience') and len(candidate['experience']) > 0:
                latest_exp = candidate['experience'][0]
                flattened['latest_job_title'] = latest_exp.get('job_title', '')
                flattened['latest_company'] = latest_exp.get('company', '')
                flattened['latest_job_description'] = latest_exp.get('description', '')
            
            # Add latest education info
            if candidate.get('education') and len(candidate['education']) > 0:
                latest_edu = candidate['education'][0]
                flattened['latest_degree'] = latest_edu.get('program', '')
                flattened['latest_institution'] = latest_edu.get('institution', '')
            
            # Add skills summary
            if candidate.get('skills'):
                skills_names = [skill.get('skill_name', '') for skill in candidate['skills']]
                flattened['skills_summary'] = ', '.join(skills_names)
            
            # Add languages summary
            if candidate.get('languages'):
                lang_info = [f"{lang.get('language', '')}: {lang.get('proficiency_level', '')}" 
                           for lang in candidate['languages']]
                flattened['languages_summary'] = ', '.join(lang_info)
            
            flattened_candidates.append(flattened)
            
        df = pd.DataFrame(flattened_candidates)
        df.to_csv(filename, index=False)
        self.logger.info(f"Saved {len(candidates)} candidates to {filename}")

async def main():
    scraper = ComprehensiveVakansiyaScraper(max_concurrent=5)
    
    print("Testing comprehensive scraper with 3 candidates...")
    candidates = await scraper.scrape_all_candidates(limit=3)
    
    if candidates:
        await scraper.save_to_json(candidates, 'test_comprehensive_candidates.json')
        scraper.save_to_csv(candidates, 'test_comprehensive_candidates.csv')
        
        print(f"\nComprehensive test completed!")
        print(f"Total candidates: {len(candidates)}")
        print(f"Files saved: test_comprehensive_candidates.json, test_comprehensive_candidates.csv")
        
        # Show detailed sample data
        if candidates:
            sample = candidates[0]
            print(f"\nDetailed sample candidate data:")
            print(f"Name: {sample.get('firstname')} {sample.get('lastname')}")
            print(f"Age: {sample.get('age')}")
            print(f"Job: {sample.get('title')}")
            print(f"Summary: {sample.get('summary')}")
            print(f"Contact: {sample.get('contact_info', {})}")
            print(f"Experience entries: {len(sample.get('experience', []))}")
            print(f"Education entries: {len(sample.get('education', []))}")
            print(f"Skills: {len(sample.get('skills', []))}")
            print(f"Languages: {len(sample.get('languages', []))}")
            print(f"Awards/Certificates: {len(sample.get('awards_certificates', []))}")
    else:
        print("No candidates were scraped.")

if __name__ == "__main__":
    asyncio.run(main())