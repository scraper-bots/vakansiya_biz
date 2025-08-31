#!/usr/bin/env python3
"""
Comprehensive script to run the Vakansiya.biz candidate scraper with full data extraction
"""
import asyncio
from vakansiya_scraper import ComprehensiveVakansiyaScraper

async def main():
    print("Starting Vakansiya.biz Comprehensive Candidate Scraper...")
    print("This scraper extracts ALL structured data from each candidate page:")
    print("- Complete contact information")
    print("- Detailed work experience with dates")
    print("- Education with institutions and degrees")
    print("- Skills with proficiency levels")
    print("- Language skills")
    print("- Awards and certificates")
    print("- Professional summary")
    
    scraper = ComprehensiveVakansiyaScraper(max_concurrent=5)
    
    print("\nScraping options:")
    print("1. Test run (first 5 candidates) - ~30 seconds")
    print("2. Small batch (25 candidates) - ~3 minutes")
    print("3. Medium batch (100 candidates) - ~12 minutes")
    print("4. Large batch (300 candidates) - ~40 minutes")
    print("5. Full scrape (~700 candidates) - ~2 hours")
    
    choice = input("Select option (1-5): ").strip()
    
    if choice == "1":
        limit = 5
        filename_base = "test"
        print(f"Running test scrape ({limit} candidates)...")
    elif choice == "2":
        limit = 25
        filename_base = "small"
        print(f"Running small scrape ({limit} candidates)...")
    elif choice == "3":
        limit = 100
        filename_base = "medium"
        print(f"Running medium scrape ({limit} candidates)...")
    elif choice == "4":
        limit = 300
        filename_base = "large"
        print(f"Running large scrape ({limit} candidates)...")
    elif choice == "5":
        limit = None
        filename_base = "full"
        confirm = input("This will scrape ALL candidates with full details (~700). This will take ~2 hours. Continue? (y/n): ").strip().lower()
        if confirm != 'y':
            print("Cancelled.")
            return
        print("Running full scrape (all candidates)...")
    else:
        print("Invalid choice.")
        return
    
    start_time = asyncio.get_event_loop().time()
    candidates = await scraper.scrape_all_candidates(limit=limit)
    end_time = asyncio.get_event_loop().time()
    
    if candidates:
        # Save in both formats
        await scraper.save_to_json(candidates, f'{filename_base}_candidates.json')
        scraper.save_to_csv(candidates, f'{filename_base}_candidates.csv')
        
        elapsed_minutes = (end_time - start_time) / 60
        
        print(f"\n🎉 Scraping completed!")
        print(f"⏱️  Time taken: {elapsed_minutes:.1f} minutes")
        print(f"📊 Total candidates: {len(candidates)}")
        print(f"📁 Files saved: {filename_base}_candidates.json, {filename_base}_candidates.csv")
        
        # Show detailed statistics
        with_summary = sum(1 for c in candidates if c.get('summary'))
        with_email = sum(1 for c in candidates if c.get('contact_info', {}).get('email'))
        with_phone = sum(1 for c in candidates if c.get('contact_info', {}).get('phone'))
        with_address = sum(1 for c in candidates if c.get('contact_info', {}).get('address'))
        with_experience = sum(1 for c in candidates if c.get('experience'))
        with_education = sum(1 for c in candidates if c.get('education'))
        with_skills = sum(1 for c in candidates if c.get('skills'))
        with_languages = sum(1 for c in candidates if c.get('languages'))
        with_awards = sum(1 for c in candidates if c.get('awards_certificates'))
        
        total_experience = sum(len(c.get('experience', [])) for c in candidates)
        total_education = sum(len(c.get('education', [])) for c in candidates)
        total_skills = sum(len(c.get('skills', [])) for c in candidates)
        total_languages = sum(len(c.get('languages', [])) for c in candidates)
        total_awards = sum(len(c.get('awards_certificates', [])) for c in candidates)
        
        print(f"\n📈 Detailed Statistics:")
        print(f"   👤 Contact Information:")
        print(f"      📧 With email: {with_email} ({with_email/len(candidates)*100:.1f}%)")
        print(f"      📞 With phone: {with_phone} ({with_phone/len(candidates)*100:.1f}%)")
        print(f"      🏠 With address: {with_address} ({with_address/len(candidates)*100:.1f}%)")
        print(f"   💼 Professional Data:")
        print(f"      📝 With summary: {with_summary} ({with_summary/len(candidates)*100:.1f}%)")
        print(f"      🏢 With experience: {with_experience} ({with_experience/len(candidates)*100:.1f}%)")
        print(f"      🎓 With education: {with_education} ({with_education/len(candidates)*100:.1f}%)")
        print(f"      🛠️  With skills: {with_skills} ({with_skills/len(candidates)*100:.1f}%)")
        print(f"      🌐 With languages: {with_languages} ({with_languages/len(candidates)*100:.1f}%)")
        print(f"      🏆 With awards: {with_awards} ({with_awards/len(candidates)*100:.1f}%)")
        print(f"   📊 Total Entries:")
        print(f"      🏢 Experience entries: {total_experience}")
        print(f"      🎓 Education entries: {total_education}")
        print(f"      🛠️  Skills entries: {total_skills}")
        print(f"      🌐 Language entries: {total_languages}")
        print(f"      🏆 Awards/Certificates: {total_awards}")
        
        # Show sample detailed data
        if candidates:
            print(f"\n🔍 Sample Detailed Candidate:")
            sample = candidates[0]
            print(f"   Name: {sample.get('firstname', 'N/A')} {sample.get('lastname', 'N/A')}")
            print(f"   Age: {sample.get('age', 'N/A')}")
            print(f"   Job: {sample.get('title', 'N/A')}")
            if sample.get('summary'):
                print(f"   Summary: {sample.get('summary')[:100]}...")
            if sample.get('contact_info'):
                contact = sample.get('contact_info')
                print(f"   📧 Email: {contact.get('email', 'N/A')}")
                print(f"   📞 Phone: {contact.get('phone', 'N/A')}")
                print(f"   🏠 Address: {contact.get('address', 'N/A')}")
            if sample.get('experience'):
                exp = sample.get('experience')[0]
                print(f"   💼 Latest Job: {exp.get('job_title', 'N/A')} at {exp.get('company', 'N/A')}")
            if sample.get('education'):
                edu = sample.get('education')[0]
                print(f"   🎓 Latest Education: {edu.get('program', 'N/A')} at {edu.get('institution', 'N/A')}")
        
    else:
        print("❌ No candidates were scraped.")

if __name__ == "__main__":
    asyncio.run(main())