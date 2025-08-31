# Vakansiya.biz Candidate Scraper

A comprehensive Python scraper to collect complete candidate data from vakansiya.biz, including both general listing information and detailed candidate profiles with full structured data extraction.

## Features

- 🚀 **High Performance**: Async/await with aiohttp for concurrent scraping
- 🔍 **Comprehensive Data**: Extracts ALL structured data from candidate pages
- 📊 **Complete Profiles**: Contact info, experience, education, skills, languages, awards
- ⚡ **Rate Limited**: Respectful scraping with built-in delays and semaphores
- 📁 **Multiple Formats**: Exports to both JSON and CSV
- 🛡️ **Error Handling**: Robust error handling with detailed logging
- 📈 **Progress Tracking**: Real-time progress updates and statistics

## Installation

1. Install required dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### 🚀 Quick Start
Extract ALL structured data from candidate pages:
```bash
python run_scraper.py
```

### 📊 Programmatic Usage
```python
import asyncio
from vakansiya_scraper import ComprehensiveVakansiyaScraper

async def main():
    scraper = ComprehensiveVakansiyaScraper(max_concurrent=5)
    
    # Test with first 5 candidates
    candidates = await scraper.scrape_all_candidates(limit=5)
    
    # Scrape all candidates with full details
    candidates = await scraper.scrape_all_candidates()
    
    # Save data
    await scraper.save_to_json(candidates, 'candidates.json')
    scraper.save_to_csv(candidates, 'candidates.csv')

asyncio.run(main())
```

## 📊 Data Structure

### 🔢 Basic API Data (from general listing)
- `id`: Unique candidate ID
- `user_id`: User ID 
- `slug`: URL slug
- `firstname`, `lastname`: Full name
- `age`: Age
- `title`: Current job title
- `industry`: Industry details (Az/En/Ru)
- `city`, `country`: Location details
- `expected_salary`: Salary expectations
- `is_premium`: Premium profile status

### 📋 Comprehensive Page Data (extracted from HTML)
- 📝 **`summary`**: Professional summary/headline
- 📞 **`contact_info`**: Complete contact details
  - Address, email, phone, marital status
  - LinkedIn, GitHub, Skype profiles
- 💼 **`experience`**: Detailed work history
  - Job titles, companies, locations
  - Start/end dates, descriptions
- 🎓 **`education`**: Education background
  - Programs, institutions, locations
  - Degree levels, dates
- 🏆 **`awards_certificates`**: Awards & certifications
  - Titles, issuers, descriptions, dates
- 🛠️ **`skills`**: Technical & soft skills
  - Skill names, proficiency levels, experience years
- 🌐 **`languages`**: Language proficiencies
  - Languages with proficiency levels

## Rate Limiting

The scraper includes built-in rate limiting:
- 1 second delay between API requests
- 2 second delay between individual page scrapes

## Output Files

- `candidates.csv`: Flattened data suitable for spreadsheet analysis
- `candidates.json`: Full structured data including nested information
- `scraper.log`: Detailed logging information

## Example URLs

- General API: `https://api.vakansiya.biz/api/v1/resumes/search`
- Individual page: `https://vakansiya.biz/az/cv/{candidate_id}/{slug}`

## 🎉 Production Results

**Real Full Scrape Results (709 candidates in 5.9 minutes):**

```
🎉 Scraping completed!
⏱️  Time taken: 5.9 minutes
📊 Total candidates: 709
📁 Files saved: full_candidates.json, full_candidates.csv

📈 Detailed Statistics:
   👤 Contact Information:
      📧 With email: 321 (45.3%)
      📞 With phone: 290 (40.9%)
      🏠 With address: 709 (100.0%)
   💼 Professional Data:
      📝 With summary: 588 (82.9%)
      🏢 With experience: 709 (100.0%)
      🎓 With education: 709 (100.0%)
      🛠️  With skills: 709 (100.0%)
      🌐 With languages: 709 (100.0%)
      🏆 With awards: 345 (48.7%)
   📊 Total Entries:
      🏢 Experience entries: 1646
      🎓 Education entries: 964
      🛠️  Skills entries: 2269
      🌐 Language entries: 1857
      🏆 Awards/Certificates: 694
```

**Performance Stats:**
- ⚡ **0.5 seconds** average per candidate
- 📊 **100%** success rate for all structured data
- 🔥 **1,646 work experience** entries extracted
- 🎓 **964 education** records collected
- 🛠️ **2,269 skills** with proficiency levels
- 🌐 **1,857 language** proficiency records
- 🏆 **694 awards/certificates** captured

## Notes

- The scraper respects the website's structure and includes appropriate delays
- All contact information and detailed data is extracted from the HTML structure
- Error handling ensures the scraper continues even if individual pages fail
- **Proven at scale**: Successfully scraped all 709 candidates with comprehensive data
- **Production files**: `full_candidates.json` and `full_candidates.csv` contain the complete dataset