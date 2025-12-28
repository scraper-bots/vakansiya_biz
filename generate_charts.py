"""
Business Analytics Report Generator for Vakansiya.biz Candidate Database
Generates business-focused visualizations for executive decision-making
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from collections import Counter
import warnings
warnings.filterwarnings('ignore')

# Set style for professional business charts
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 7)
plt.rcParams['font.size'] = 11
plt.rcParams['axes.labelsize'] = 12
plt.rcParams['axes.titlesize'] = 14
plt.rcParams['xtick.labelsize'] = 10
plt.rcParams['ytick.labelsize'] = 10

# Color palette for business charts
BUSINESS_COLORS = ['#2E86AB', '#A23B72', '#F18F01', '#C73E1D', '#6A994E',
                   '#BC4B51', '#8B8C89', '#5E60CE', '#F72585', '#4361EE']

def load_data():
    """Load and prepare the candidate data"""
    df = pd.read_csv('full_candidates.csv')
    print(f"Loaded {len(df)} candidate records")
    return df

def clean_data(df):
    """Clean and prepare data for analysis"""
    # Convert age to numeric
    df['age'] = pd.to_numeric(df['age'], errors='coerce')

    # Convert expected_salary to numeric
    df['expected_salary'] = pd.to_numeric(df['expected_salary'], errors='coerce')

    # Convert counts to numeric
    for col in ['experience_count', 'education_count', 'awards_count', 'skills_count', 'languages_count']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Fill NaN values appropriately
    df['is_premium'] = df['is_premium'].fillna(0).astype(int)

    return df

def save_chart(filename):
    """Save chart with consistent formatting"""
    plt.tight_layout()
    plt.savefig(f'charts/{filename}', dpi=300, bbox_inches='tight')
    plt.close()
    print(f"✓ Generated: {filename}")

# ============================================================================
# CHART 1: Age Distribution of Candidates
# ============================================================================
def chart_age_distribution(df):
    """Workforce age demographics"""
    fig, ax = plt.subplots(figsize=(12, 7))

    # Remove outliers and NaN
    age_data = df['age'].dropna()
    age_data = age_data[(age_data >= 18) & (age_data <= 65)]

    # Create age bins
    bins = [18, 22, 25, 30, 35, 40, 45, 50, 65]
    labels = ['18-22', '23-25', '26-30', '31-35', '36-40', '41-45', '46-50', '50+']

    age_groups = pd.cut(age_data, bins=bins, labels=labels, include_lowest=True)
    age_counts = age_groups.value_counts().sort_index()

    bars = ax.bar(range(len(age_counts)), age_counts.values, color=BUSINESS_COLORS[0], alpha=0.8)
    ax.set_xticks(range(len(age_counts)))
    ax.set_xticklabels(age_counts.index, rotation=0)
    ax.set_xlabel('Age Group')
    ax.set_ylabel('Number of Candidates')
    ax.set_title('Candidate Age Distribution: Workforce Demographics Analysis', fontweight='bold', pad=20)

    # Add value labels on bars
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height)}',
                ha='center', va='bottom', fontweight='bold')

    # Add median age annotation
    median_age = age_data.median()
    ax.text(0.98, 0.95, f'Median Age: {median_age:.0f} years',
            transform=ax.transAxes, ha='right', va='top',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5),
            fontsize=11, fontweight='bold')

    save_chart('01_age_distribution.png')

# ============================================================================
# CHART 2: Top Industries by Candidate Count
# ============================================================================
def chart_top_industries(df):
    """Industry talent concentration"""
    fig, ax = plt.subplots(figsize=(14, 8))

    industry_counts = df['industry_title_en'].value_counts().head(15)

    bars = ax.barh(range(len(industry_counts)), industry_counts.values, color=BUSINESS_COLORS[1], alpha=0.8)
    ax.set_yticks(range(len(industry_counts)))
    ax.set_yticklabels(industry_counts.index)
    ax.set_xlabel('Number of Candidates')
    ax.set_title('Top 15 Industries by Candidate Availability: Market Talent Concentration',
                 fontweight='bold', pad=20)
    ax.invert_yaxis()

    # Add value labels
    for i, bar in enumerate(bars):
        width = bar.get_width()
        percentage = (width / len(df)) * 100
        ax.text(width, bar.get_y() + bar.get_height()/2.,
                f' {int(width)} ({percentage:.1f}%)',
                ha='left', va='center', fontweight='bold')

    save_chart('02_top_industries.png')

# ============================================================================
# CHART 3: Expected Salary Distribution
# ============================================================================
def chart_salary_distribution(df):
    """Compensation expectations analysis"""
    fig, ax = plt.subplots(figsize=(12, 7))

    # Filter out 0 salaries and outliers
    salary_data = df[df['expected_salary'] > 0]['expected_salary']
    salary_data = salary_data[salary_data <= salary_data.quantile(0.95)]

    if len(salary_data) > 0:
        # Create salary bins
        bins = [0, 500, 1000, 1500, 2000, 2500, 3000, 5000]
        labels = ['<500', '500-1K', '1K-1.5K', '1.5K-2K', '2K-2.5K', '2.5K-3K', '3K+']

        salary_groups = pd.cut(salary_data, bins=bins, labels=labels, include_lowest=True)
        salary_counts = salary_groups.value_counts().sort_index()

        bars = ax.bar(range(len(salary_counts)), salary_counts.values, color=BUSINESS_COLORS[2], alpha=0.8)
        ax.set_xticks(range(len(salary_counts)))
        ax.set_xticklabels(salary_counts.index, rotation=45, ha='right')
        ax.set_xlabel('Expected Salary Range (AZN)')
        ax.set_ylabel('Number of Candidates')
        ax.set_title('Expected Salary Distribution: Market Compensation Expectations',
                     fontweight='bold', pad=20)

        # Add value labels
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                    f'{int(height)}',
                    ha='center', va='bottom', fontweight='bold')

        # Add statistics
        median_salary = salary_data.median()
        mean_salary = salary_data.mean()
        stats_text = f'Median: {median_salary:.0f} AZN\nMean: {mean_salary:.0f} AZN'
        ax.text(0.98, 0.95, stats_text,
                transform=ax.transAxes, ha='right', va='top',
                bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.5),
                fontsize=11, fontweight='bold')

    save_chart('03_salary_distribution.png')

# ============================================================================
# CHART 4: Premium vs Non-Premium Candidates
# ============================================================================
def chart_premium_breakdown(df):
    """Premium membership conversion opportunity"""
    fig, ax = plt.subplots(figsize=(10, 7))

    premium_counts = df['is_premium'].value_counts()

    # Handle case where only one category exists
    if len(premium_counts) == 1:
        # All candidates are either premium or non-premium
        if 0 in premium_counts.index:
            # All are non-premium
            categories = ['Standard', 'Premium (Target)']
            values = [premium_counts[0], 0]
            colors = [BUSINESS_COLORS[3], BUSINESS_COLORS[4]]
            is_all_standard = True
        else:
            # All are premium (unlikely but handle it)
            categories = ['Standard', 'Premium']
            values = [0, premium_counts[1]]
            colors = [BUSINESS_COLORS[3], BUSINESS_COLORS[4]]
            is_all_standard = False
    else:
        categories = ['Standard', 'Premium']
        values = [premium_counts.get(0, 0), premium_counts.get(1, 0)]
        colors = [BUSINESS_COLORS[3], BUSINESS_COLORS[4]]
        is_all_standard = False

    bars = ax.bar(range(len(categories)), values, color=colors, alpha=0.8)
    ax.set_xticks(range(len(categories)))
    ax.set_xticklabels(categories)
    ax.set_ylabel('Number of Candidates')
    ax.set_title('Premium vs Standard Membership: Monetization Opportunity Analysis',
                 fontweight='bold', pad=20)

    # Add value labels and percentages
    total = len(df)
    for i, bar in enumerate(bars):
        height = bar.get_height()
        if height > 0:
            percentage = (height / total) * 100
            ax.text(bar.get_x() + bar.get_width()/2., height,
                    f'{int(height)}\n({percentage:.1f}%)',
                    ha='center', va='bottom', fontweight='bold', fontsize=12)
        else:
            ax.text(bar.get_x() + bar.get_width()/2., 10,
                    f'0\n(0%)',
                    ha='center', va='bottom', fontweight='bold', fontsize=12)

    # Calculate conversion opportunity
    if is_all_standard:
        potential_revenue = values[0]
        ax.text(0.98, 0.95, f'MASSIVE OPPORTUNITY!\nAll {int(potential_revenue)} candidates\nare potential premium conversions',
                transform=ax.transAxes, ha='right', va='top',
                bbox=dict(boxstyle='round', facecolor='yellow', alpha=0.5),
                fontsize=11, fontweight='bold')
    else:
        potential_revenue = values[0]
        if potential_revenue > 0:
            ax.text(0.98, 0.95, f'Conversion Opportunity:\n{int(potential_revenue)} candidates',
                    transform=ax.transAxes, ha='right', va='top',
                    bbox=dict(boxstyle='round', facecolor='yellow', alpha=0.3),
                    fontsize=11, fontweight='bold')

    save_chart('04_premium_breakdown.png')

# ============================================================================
# CHART 5: Experience Level Distribution
# ============================================================================
def chart_experience_distribution(df):
    """Talent maturity analysis"""
    fig, ax = plt.subplots(figsize=(12, 7))

    exp_data = df['experience_count'].dropna()
    exp_data = exp_data[exp_data <= 20]  # Remove outliers

    # Create experience bins
    bins = [0, 1, 2, 3, 5, 10, 20]
    labels = ['Entry (0-1)', 'Junior (1-2)', 'Mid (2-3)', 'Senior (3-5)', 'Expert (5-10)', 'Master (10+)']

    exp_groups = pd.cut(exp_data, bins=bins, labels=labels, include_lowest=True)
    exp_counts = exp_groups.value_counts().sort_index()

    bars = ax.bar(range(len(exp_counts)), exp_counts.values, color=BUSINESS_COLORS[5], alpha=0.8)
    ax.set_xticks(range(len(exp_counts)))
    ax.set_xticklabels(exp_counts.index, rotation=45, ha='right')
    ax.set_xlabel('Experience Level (Years)')
    ax.set_ylabel('Number of Candidates')
    ax.set_title('Experience Level Distribution: Talent Maturity Landscape',
                 fontweight='bold', pad=20)

    # Add value labels
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height)}',
                ha='center', va='bottom', fontweight='bold')

    save_chart('05_experience_distribution.png')

# ============================================================================
# CHART 6: Education Level Distribution
# ============================================================================
def chart_education_distribution(df):
    """Qualification landscape"""
    fig, ax = plt.subplots(figsize=(10, 7))

    edu_data = df['education_count'].dropna()
    edu_data = edu_data[edu_data <= 5]

    edu_counts = edu_data.value_counts().sort_index()

    bars = ax.bar(range(len(edu_counts)), edu_counts.values, color=BUSINESS_COLORS[6], alpha=0.8)
    ax.set_xticks(range(len(edu_counts)))
    ax.set_xticklabels([f'{int(x)} degree(s)' for x in edu_counts.index])
    ax.set_xlabel('Number of Education Degrees')
    ax.set_ylabel('Number of Candidates')
    ax.set_title('Education Level Distribution: Qualification Landscape',
                 fontweight='bold', pad=20)

    # Add value labels
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height)}',
                ha='center', va='bottom', fontweight='bold')

    save_chart('06_education_distribution.png')

# ============================================================================
# CHART 7: Top Skills in Demand
# ============================================================================
def chart_top_skills(df):
    """In-demand skills market analysis"""
    fig, ax = plt.subplots(figsize=(14, 8))

    # Extract all skills from skills_summary
    all_skills = []
    for skills in df['skills_summary'].dropna():
        if isinstance(skills, str):
            skill_list = [s.strip() for s in skills.split(',')]
            all_skills.extend(skill_list)

    # Count skills
    skill_counts = Counter(all_skills)
    top_skills = dict(skill_counts.most_common(15))

    if top_skills:
        bars = ax.barh(range(len(top_skills)), list(top_skills.values()),
                      color=BUSINESS_COLORS[7], alpha=0.8)
        ax.set_yticks(range(len(top_skills)))
        ax.set_yticklabels(list(top_skills.keys()))
        ax.set_xlabel('Number of Candidates with Skill')
        ax.set_title('Top 15 Skills in Demand: Market Skills Analysis',
                     fontweight='bold', pad=20)
        ax.invert_yaxis()

        # Add value labels
        for bar in bars:
            width = bar.get_width()
            ax.text(width, bar.get_y() + bar.get_height()/2.,
                    f' {int(width)}',
                    ha='left', va='center', fontweight='bold')

    save_chart('07_top_skills.png')

# ============================================================================
# CHART 8: Language Proficiency Distribution
# ============================================================================
def chart_language_distribution(df):
    """Language capabilities in the workforce"""
    fig, ax = plt.subplots(figsize=(12, 7))

    lang_data = df['languages_count'].dropna()
    lang_data = lang_data[lang_data <= 6]

    lang_counts = lang_data.value_counts().sort_index()

    bars = ax.bar(range(len(lang_counts)), lang_counts.values, color=BUSINESS_COLORS[8], alpha=0.8)
    ax.set_xticks(range(len(lang_counts)))
    ax.set_xticklabels([f'{int(x)} language(s)' for x in lang_counts.index])
    ax.set_xlabel('Number of Languages Spoken')
    ax.set_ylabel('Number of Candidates')
    ax.set_title('Language Proficiency Distribution: Multilingual Talent Pool',
                 fontweight='bold', pad=20)

    # Add value labels
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height)}',
                ha='center', va='bottom', fontweight='bold')

    # Add average
    avg_langs = lang_data.mean()
    ax.text(0.98, 0.95, f'Average: {avg_langs:.1f} languages',
            transform=ax.transAxes, ha='right', va='top',
            bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.5),
            fontsize=11, fontweight='bold')

    save_chart('08_language_distribution.png')

# ============================================================================
# CHART 9: Geographic Distribution
# ============================================================================
def chart_city_distribution(df):
    """Geographic talent concentration"""
    fig, ax = plt.subplots(figsize=(12, 7))

    city_counts = df['city'].value_counts().head(10)

    bars = ax.bar(range(len(city_counts)), city_counts.values, color=BUSINESS_COLORS[9], alpha=0.8)
    ax.set_xticks(range(len(city_counts)))
    ax.set_xticklabels(city_counts.index, rotation=45, ha='right')
    ax.set_xlabel('City')
    ax.set_ylabel('Number of Candidates')
    ax.set_title('Top 10 Cities by Candidate Location: Geographic Talent Distribution',
                 fontweight='bold', pad=20)

    # Add value labels and percentages
    total = len(df)
    for bar in bars:
        height = bar.get_height()
        percentage = (height / total) * 100
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height)}\n({percentage:.1f}%)',
                ha='center', va='bottom', fontweight='bold')

    save_chart('09_city_distribution.png')

# ============================================================================
# CHART 10: Digital Presence Analysis
# ============================================================================
def chart_digital_presence(df):
    """Professional digital footprint"""
    fig, ax = plt.subplots(figsize=(10, 7))

    linkedin_count = df['linkedin'].notna().sum()
    github_count = df['github'].notna().sum()
    both_count = ((df['linkedin'].notna()) & (df['github'].notna())).sum()
    neither_count = ((df['linkedin'].isna()) & (df['github'].isna())).sum()

    categories = ['LinkedIn Only', 'GitHub Only', 'Both', 'Neither']
    linkedin_only = linkedin_count - both_count
    github_only = github_count - both_count
    counts = [linkedin_only, github_only, both_count, neither_count]

    colors_custom = [BUSINESS_COLORS[0], BUSINESS_COLORS[1], BUSINESS_COLORS[4], BUSINESS_COLORS[3]]
    bars = ax.bar(range(len(categories)), counts, color=colors_custom, alpha=0.8)
    ax.set_xticks(range(len(categories)))
    ax.set_xticklabels(categories, rotation=0)
    ax.set_ylabel('Number of Candidates')
    ax.set_title('Professional Digital Presence: LinkedIn & GitHub Profile Analysis',
                 fontweight='bold', pad=20)

    # Add value labels and percentages
    total = len(df)
    for i, bar in enumerate(bars):
        height = bar.get_height()
        percentage = (height / total) * 100
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height)}\n({percentage:.1f}%)',
                ha='center', va='bottom', fontweight='bold')

    save_chart('10_digital_presence.png')

# ============================================================================
# CHART 11: Industry vs Average Experience
# ============================================================================
def chart_industry_experience(df):
    """Sector maturity analysis"""
    fig, ax = plt.subplots(figsize=(14, 9))

    # Calculate average experience per industry
    industry_exp = df.groupby('industry_title_en')['experience_count'].agg(['mean', 'count'])
    industry_exp = industry_exp[industry_exp['count'] >= 10]  # Only industries with 10+ candidates
    industry_exp = industry_exp.sort_values('mean', ascending=True).tail(12)

    bars = ax.barh(range(len(industry_exp)), industry_exp['mean'].values,
                  color=BUSINESS_COLORS[5], alpha=0.8)
    ax.set_yticks(range(len(industry_exp)))
    ax.set_yticklabels(industry_exp.index)
    ax.set_xlabel('Average Years of Experience')
    ax.set_title('Top Industries by Average Experience: Sector Maturity Analysis',
                 fontweight='bold', pad=20)
    ax.invert_yaxis()

    # Add value labels with candidate counts
    for i, bar in enumerate(bars):
        width = bar.get_width()
        count = int(industry_exp.iloc[i]['count'])
        ax.text(width, bar.get_y() + bar.get_height()/2.,
                f' {width:.1f} years (n={count})',
                ha='left', va='center', fontweight='bold')

    save_chart('11_industry_experience.png')

# ============================================================================
# CHART 12: Marital Status Distribution
# ============================================================================
def chart_marital_status(df):
    """Demographic profile analysis"""
    fig, ax = plt.subplots(figsize=(10, 7))

    marital_counts = df['marital_status'].value_counts()

    bars = ax.bar(range(len(marital_counts)), marital_counts.values,
                 color=BUSINESS_COLORS[6], alpha=0.8)
    ax.set_xticks(range(len(marital_counts)))
    ax.set_xticklabels(marital_counts.index, rotation=0)
    ax.set_ylabel('Number of Candidates')
    ax.set_title('Marital Status Distribution: Candidate Demographic Profile',
                 fontweight='bold', pad=20)

    # Add value labels and percentages
    total = marital_counts.sum()
    for bar in bars:
        height = bar.get_height()
        percentage = (height / total) * 100
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height)}\n({percentage:.1f}%)',
                ha='center', va='bottom', fontweight='bold')

    save_chart('12_marital_status.png')

# ============================================================================
# CHART 13: Skills Count Distribution
# ============================================================================
def chart_skills_count(df):
    """Professional capabilities breadth"""
    fig, ax = plt.subplots(figsize=(12, 7))

    skills_data = df['skills_count'].dropna()
    skills_data = skills_data[skills_data <= 15]

    # Create bins
    bins = [0, 1, 3, 5, 7, 10, 15]
    labels = ['1 skill', '2-3 skills', '4-5 skills', '6-7 skills', '8-10 skills', '10+ skills']

    skills_groups = pd.cut(skills_data, bins=bins, labels=labels, include_lowest=True)
    skills_counts = skills_groups.value_counts().sort_index()

    bars = ax.bar(range(len(skills_counts)), skills_counts.values,
                 color=BUSINESS_COLORS[7], alpha=0.8)
    ax.set_xticks(range(len(skills_counts)))
    ax.set_xticklabels(skills_counts.index, rotation=45, ha='right')
    ax.set_xlabel('Number of Skills Listed')
    ax.set_ylabel('Number of Candidates')
    ax.set_title('Skills Portfolio Distribution: Professional Capabilities Breadth',
                 fontweight='bold', pad=20)

    # Add value labels
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height)}',
                ha='center', va='bottom', fontweight='bold')

    save_chart('13_skills_count.png')

# ============================================================================
# CHART 14: Awards Distribution
# ============================================================================
def chart_awards_distribution(df):
    """Recognition and achievement analysis"""
    fig, ax = plt.subplots(figsize=(10, 7))

    awards_data = df['awards_count'].dropna()
    awards_data = awards_data[awards_data <= 5]

    awards_counts = awards_data.value_counts().sort_index()

    bars = ax.bar(range(len(awards_counts)), awards_counts.values,
                 color=BUSINESS_COLORS[2], alpha=0.8)
    ax.set_xticks(range(len(awards_counts)))
    ax.set_xticklabels([f'{int(x)} award(s)' for x in awards_counts.index])
    ax.set_xlabel('Number of Awards/Certifications')
    ax.set_ylabel('Number of Candidates')
    ax.set_title('Awards & Certifications Distribution: Achievement Recognition Analysis',
                 fontweight='bold', pad=20)

    # Add value labels
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height)}',
                ha='center', va='bottom', fontweight='bold')

    # Calculate percentage with awards
    with_awards = (df['awards_count'] > 0).sum()
    pct_with_awards = (with_awards / len(df)) * 100
    ax.text(0.98, 0.95, f'{pct_with_awards:.1f}% have awards/certifications',
            transform=ax.transAxes, ha='right', va='top',
            bbox=dict(boxstyle='round', facecolor='gold', alpha=0.3),
            fontsize=11, fontweight='bold')

    save_chart('14_awards_distribution.png')

# ============================================================================
# MAIN EXECUTION
# ============================================================================
def main():
    """Generate all business analytics charts"""
    print("\n" + "="*70)
    print("VAKANSIYA.BIZ CANDIDATE DATABASE - BUSINESS ANALYTICS REPORT")
    print("="*70 + "\n")

    # Load and prepare data
    df = load_data()
    df = clean_data(df)

    print(f"\nGenerating business intelligence visualizations...\n")

    # Generate all charts
    chart_age_distribution(df)
    chart_top_industries(df)
    chart_salary_distribution(df)
    chart_premium_breakdown(df)
    chart_experience_distribution(df)
    chart_education_distribution(df)
    chart_top_skills(df)
    chart_language_distribution(df)
    chart_city_distribution(df)
    chart_digital_presence(df)
    chart_industry_experience(df)
    chart_marital_status(df)
    chart_skills_count(df)
    chart_awards_distribution(df)

    print("\n" + "="*70)
    print("✓ ALL CHARTS GENERATED SUCCESSFULLY")
    print("="*70)
    print(f"\nLocation: charts/ directory")
    print(f"Total charts: 14")
    print("\nNext step: Review README.md for business insights and findings\n")

if __name__ == "__main__":
    main()
