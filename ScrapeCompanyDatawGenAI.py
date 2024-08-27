import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from langdetect import detect
import time
import re
import pandas as pd
import openai
import json
from retry import retry


# Function to detect if text is English
def is_english(text):
    try:
        return detect(text) == 'en' and '�' not in text
    except:
        return False

# Function to clean and normalize text
def clean_text(text):
    return re.sub(r'\s+', ' ', text).strip()

# Function to extract text from a webpage
def extract_text_from_url(url):
    try:
        response = requests.get(url)
        if response.status_code == 403:
            headers = {
                "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
            }
            time.sleep(0.1)  # Wait before retrying
            response = requests.get(url, headers=headers)

        if response.status_code != 200:
            print(f"Failed to fetch {url}, status code: {response.status_code}")
            return None
        soup = BeautifulSoup(response.content, 'html.parser')
        text = soup.get_text(separator=' ', strip=True)
        segments = set()
        for segment in re.split(r'[.?!]', text):
            cleaned_segment = clean_text(segment)
            if is_english(cleaned_segment) and cleaned_segment:
                segments.add(cleaned_segment)
        return ' '.join(segments)
    except Exception as e:
        print(f"Failed to extract text from {url}: {str(e)}")
        return None

# Function to exclude urls based on keywords
def urls_to_exclude(url, word_list):
    url_lower = url.lower()
    return not any(word.lower() in url_lower for word in word_list)

# Function to crawl a website and extract text from all pages
def crawl_website(base_url, max_pages=50):
    visited = set()
    to_visit = [base_url]
    all_texts = []
    url_keywords_to_exclude = ['privacy-policy', 'career', 'youtube', 'instagram', 'facebook', 'twitter', '.pdf', 'jpg',
                               '.jpeg', '.png', '.gif', 'mailto', 'tel:']
    base_netloc = urlparse(base_url).netloc

    while to_visit and len(visited) < max_pages:
        current_url = to_visit.pop(0)

        if current_url in visited:
            continue

        visited.add(current_url)
        text = extract_text_from_url(current_url)
        if text:
            all_texts.append(text + '\t' + current_url)
        try:
            response = requests.get(current_url)
            if response.status_code == 403:
                headers = {
                    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
                }
                time.sleep(0.1)  # Wait before retrying
                response = requests.get(current_url, headers=headers)

            if response.status_code != 200:
                print(f"Failed to fetch {current_url}, status code: {response.status_code}")
                return None
            soup = BeautifulSoup(response.content, 'html.parser')

            for link in soup.find_all('a', href=True):
                full_url = urljoin(base_url, link['href'])
                if (urlparse(full_url).netloc == base_netloc and
                        full_url not in visited and
                        full_url not in to_visit and
                        (urls_to_exclude(full_url, url_keywords_to_exclude) or full_url == base_url)):
                    to_visit.append(full_url)
            time.sleep(1)

        except Exception as e:
            print(f"Failed to crawl {current_url}: {str(e)}")

    return all_texts

# Function to process the extracted text using OpenAI's API
@retry(tries=3, delay=2)
def process_text_with_openai(text):
    template =  """{
  "description": "Insert a short description outlining the company's technology and main activities here.",
  "hq_and_offices": [
    {
      "location": "Insert the location of the office here.",
      "is_hq": "Y"  // Use 'Y' only if this location is the headquarters, otherwise use 'N'.
    }
    // Add more office locations as needed
  ],
  "clients": [
    "Insert the first client here",
    "Insert the second client here",
    "Insert the third client here",
    "Insert more clients as needed"
  ],
  "news": [
    {
      "news_title": "Insert the news title here",
      "news_date": "Insert the news date here (YYYY-MM-DD)",
      "news_url": "Insert the URL for the news article here",
      "news_summary": "Insert a short summary of the news here"
    }
    // Add more news items as needed
  ]
}
"""
    prompt = f"""
    Extract the following information from the text, as I am going to load it into a database. Provide very specific and standard output as JSON:
    and dont include any comments in json
    {template}
   
    Text:
    {text}

    Provide the extracted information in a structured format.
    """

    response = openai.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": prompt}
        ]
    )

    # Get the content of the message
    structured_info = response.choices[0].message.content.strip()

    # Remove any code fences if present
    if structured_info.startswith("```json"):
        structured_info = structured_info.strip('```json\n').strip('```')

    # Attempt to parse the structured information as JSON
    try:
        structured_info_json = json.loads(structured_info)
    except json.JSONDecodeError as e:
        print("Failed to decode JSON:", e)
        return None

    # Check if required keys are present
    required_keys = ['description', 'hq_and_offices', 'clients', 'news']
    for key in required_keys:
        if key not in structured_info_json:
            print(f"Missing key: {key}")
            raise Exception(f"Missing key: {key} in the JSON response")

    return structured_info_json

# Set your OpenAI API key
openai.api_key = 'open_ai_api_key'

# Load companies from a JSON file
with open('companies.json', 'r') as file:
    companies = json.load(file)

description_data = []
location_data = []
clients_data = []
news_data = []

# Initialize Excel writer
writer = pd.ExcelWriter('company_data.xlsx', engine='xlsxwriter')

for company in companies:
    print(f"Processing {company['company_name']}...")
    all_texts = crawl_website(company['company_website'])
    if all_texts:
        combined_text = ' '.join(all_texts)
        if len(combined_text) > 200000:
            combined_text = combined_text[:200000]
        structured_info = process_text_with_openai(combined_text)
        # Append description data
        description_data.append({"company_id": company["company_id"], "description": structured_info['description']})

        # Append HQ and offices data
        for location in structured_info['hq_and_offices']:
            location_data.append({
                "company_id": company["company_id"],
                "location": location["location"],
                "is_hq": location["is_hq"],
            })

        # Append clients data
        for client in structured_info['clients']:
            clients_data.append({"company_id": company["company_id"], "client": client})

        # Append news data
        for news_item in structured_info['news']:
            news_data.append({
                'company_id': company['company_id'],
                'news_title': news_item['news_title'],
                'news_date': news_item['news_date'],
                'news_url': news_item['news_url'],
                'news_summary': news_item['news_summary']
            })

    # Write data to Excel file after processing each company
    pd.DataFrame(description_data).to_excel(writer, sheet_name='description', index=False)
    pd.DataFrame(location_data).to_excel(writer, sheet_name='location', index=False)
    pd.DataFrame(clients_data).to_excel(writer, sheet_name='clients', index=False)
    pd.DataFrame(news_data).to_excel(writer, sheet_name='news', index=False)

# Save and close the Excel file
writer.close()

print("Data has been saved to company_data.xlsx")
