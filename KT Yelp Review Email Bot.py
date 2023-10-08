#!/usr/bin/env python
# coding: utf-8

# # KT Yelp Email Bot

# In[1]:


# set to run every saturday at 9am PST


# In[2]:


import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from bs4 import BeautifulSoup
import re
import html
import json
from datetime import datetime, timedelta

import random

from mailjet_rest import Client


# In[3]:


locations_df_temp = pd.read_excel("KT Locations Data.xlsx")
locations_df = locations_df_temp.copy()
business_ids = locations_df['Yelp_Bus_Id'].tolist()
reviews_data = []

current_date = datetime.now()
three_months_ago = current_date - timedelta(days=90)

email_1 = pd.read_csv("Emails.txt").columns[0]
email_2 = pd.read_csv("Emails.txt").columns[1]

MAIL_API_KEY = pd.read_csv("MAIL_API_KEY.txt").columns[0]
MAIL_API_KEY_S = pd.read_csv("MAIL_API_KEY.txt").iloc[0, 0]


# ## Web Scraping

# In[4]:


# given a 'list', list_search searches for the given string 'term' and will output whatever is in the position 
# 'num' off of the position of 'term'
def list_search(list, term, num):
    
    indices = [index for index, item in enumerate(list) if item == term]
    index = indices[0]
    target_val = list[index + num]
    
    return target_val


# In[5]:


# pulls the 10 most recent reviews from yelp page given business_id and outputs them as a df
def yelp_review_scraper(business_id):   
    search_url = f"https://www.yelp.com/biz/{business_id}?sort_by=date_desc"
    search_response = requests.get(search_url)

    # Check if the request was successful (status code 200)
    if search_response.status_code == 200:
        soup = BeautifulSoup(search_response.text, 'html.parser')

    # find matches for set pattern
    soup_string = str(soup)
    soup_string = soup_string.replace('null', '"%"')
    pattern = r'"reviews":\[(.*?)\](.*?)\](.*?)\[(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\[(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\[(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\[(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\[(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\[(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\[(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\[(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\[(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\[(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\[(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\[(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\[(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\[(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\[(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\[(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\[(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\[(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\[(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\}(.*?)\['
    matches = re.findall(pattern, soup_string)

    # convert tuple matches in match to list
    matches_new = [list(t) for t in matches]

    # cut lists at stop list to get only the data we need
    stop_phrase = '"tags":'

    new_list = []
    current_sublist = []

    for item in matches_new[0]:
        if stop_phrase in item:
            if current_sublist:
                current_sublist.append(item)
                new_list.append(current_sublist)
            current_sublist = []
        else:
            current_sublist.append(item)

    if current_sublist:
        new_list.append(current_sublist)

    # handle the issue of nested lists
    too_many_lists = [["".join(sublist)] for sublist in new_list]
    flattened_list = [item for sublist in too_many_lists for item in sublist]
    filtered_list = [item for item in flattened_list if stop_phrase in item]

    # extract the data we actually want from individual review JSON data
    names = []
    review_texts = []
    dates = []
    ratings = []
    review_ids = []
    already_replieds = []

    for match in filtered_list:
                filtered_list = match.split('"')
                filtered_list = [item for item in filtered_list if item != ',']
                name = list_search(filtered_list, 'markupDisplayName', 2)
                review_text = list_search(filtered_list, 'text', 2)
                date = datetime.strptime(list_search(filtered_list, 'localizedDate', 2), '%m/%d/%Y')
                rating = int(list_search(filtered_list, 'rating', 1).strip(':,'))
                review_id = list_search(filtered_list, 'id', 2)
            
                businessOwnerReplies = list_search(filtered_list, 'businessOwnerReplies', 2)
            
                if businessOwnerReplies == '%':
                    already_replied = False
                else:
                    already_replied = True
                names.append(name)
                review_texts.append(review_text)
                dates.append(date)
                ratings.append(rating)
                review_ids.append(review_id)
                already_replieds.append(already_replied)
            
    data = {
        "names": names,
        "review_text": review_texts,
        "date": dates,
        "rating": ratings,
        "review_id": review_ids,
        "already_replied": already_replieds
    }

    df = pd.DataFrame(data)

    return df           


# In[6]:


max_attempts = 3  
for _ in range(max_attempts):
    try:
        dfs = []
        
        # Loop through each business_id
        for business_id in business_ids:
            df = yelp_review_scraper(business_id)
            if df is not None:
                df['business_id'] = business_id
                dfs.append(df)
                
        break  
    except Exception as e:
        print(f"An error occurred: {e}")


# In[7]:


# Concatenate all the DataFrames in the list
yelp_reviews = pd.concat(dfs, ignore_index=True)

# Replace business_id with Clinic in df
merged_df = yelp_reviews.merge(locations_df[['Yelp_Bus_Id', 'Clinic']], left_on='business_id', right_on='Yelp_Bus_Id', how='left')

# Drop the 'business_id' column and rename the 'Clinic' column
merged_df.drop(columns=['business_id'], inplace=True)
merged_df.rename(columns={'Yelp_Bus_Id': 'business_id'}, inplace=True)
yelp_reviews = merged_df

yelp_reviews['review_text'] = yelp_reviews['review_text'].apply(lambda x: x.replace('\xa0', ''))
yelp_reviews['review_text'] = yelp_reviews['review_text'].apply(lambda x: x.replace('&amp;#39;', "'"))
yelp_reviews['review_text'] = yelp_reviews['review_text'].apply(lambda x: x.replace('<br&gt;', ''))
yelp_reviews['review_text'] = yelp_reviews['review_text'].apply(lambda x: x.replace('&amp;#34;', '"'))
yelp_reviews['review_text'] = yelp_reviews['review_text'].apply(lambda x: x.replace('&amp;amp;', '&'))
yelp_reviews['review_text_org'] = yelp_reviews['review_text']
yelp_reviews['review_text'] = yelp_reviews['review_text'].str.lower()



# ## Generate Review Responses

# In[9]:


#review prompt banks
Five_Star = ["Thank you for visiting us, [Name]! Your kind words brighten our day. Should you ever need our services in the future, please don't hesitate to reach out. Best regards, Dr. De Silva",
             "Wow, [Name], we can't thank you enough for your generous feedback! It truly warms our hearts. Feel free to return whenever you require our care. Warm regards, Dr. De Silva", 
             "Dear [Name], we're immensely grateful for your support and the wonderful review. Your children's well-being is our top priority, so please remember that we're here for you whenever you need us. Wishing you all the best, Dr. De Silva",
             "Your kind words mean the world to us, [Name]. We're here to provide the best care possible for your children. If you ever require our assistance again, please reach out. Thank you, Dr. De Silva",
             "Thank you so much, [Name], for your heartwarming review! Please know that you're always welcome back whenever the need arises. And don't forget, you can easily schedule appointments online through our portal at ktdoctor.com. Best wishes, Dr. De Silva",
             "[Name], your positive feedback is greatly appreciated. We'll be sure to share your kind words with our team. If you have any further questions or require our services, please don't hesitate to contact us. Warm regards, Dr. De Silva",
             "Thank you for your review; it means a lot to us. For any inquiries or assistance, please feel free to connect with our office at lacanada@ktdoctor.com. We look forward to continuing to provide you with exceptional care. Kind regards, Dr. De Silva",
             "Your support is invaluable, [Name]! We can't wait to welcome you and your children back whenever you need us. Your well-being is our priority. Stay safe and reach out to us anytime at lacanada@ktdoctor.com. Warm regards, Dr. De Silva",
             "Thank You, [Name]! Your kind words motivate us to keep providing top-notch care. As your children grow, know that we're here to support you along the way. Best, Dr. De Silva, drdesilva@ktdoctor.com"]
One_Star = ["I'm truly sorry for the inconvenience you experienced during your visit. Your feedback is important to us. Please reach out to us at drdesilva@ktdoctor.com, and we'll do our best to address your concerns and provide you with better service. Thank you for bringing this to our attention.",
            "We're genuinely sorry to hear about your negative experience. Please contact us at drdesilva@ktdoctor.com. Thank you for your review.",
            "I apologize for the experience you had with us. We genuinely want to assist you better and address your concerns. Please email us directly at drdesilva@ktdoctor.com with your contact information, and I will personally reach out to you. Thank you for bringing this to our attention.",
            "Hello [Name], I'm sincerely sorry for the negative experience you encountered. Your feedback is crucial to us, and we want to make things right. You can contact us 24/7 for non-urgent matters by texting 626-298-7121 or emailing drdesilva@ktdoctor.com. We are committed to improving your experience.",
            "I'm truly sorry that you had a disappointing experience with us. We value your feedback, and we're dedicated to making improvements. Please email me directly at drdesilva@ktdoctor.com. Your satisfaction is our priority, and we appreciate your review."]

T_F_Star = ["Hello [Name], thank you for sharing your thoughts about your recent experience with us. We appreciate your input and take every review seriously. Your feedback helps us continually improve our services. If you have any additional insights or suggestions, please feel free to share them with us at drdesilva@ktdoctor.com. We're here to serve you better.",
            "Dear [Name], we're grateful for your review and for choosing our practice for your child's healthcare needs. Your feedback is important to us, and it helps us better understand our patients' experiences. If there are any specific areas you'd like us to focus on or if you have more details to provide, please don't hesitate to reach out to us at drdesilva@ktdoctor.com. Thank you for entrusting us with your child's care.",
            "Hi [Name], thank you for taking the time to leave your feedback about our practice. We're glad to hear about your recent experience with our care. We value all feedback, and it's important in our ongoing efforts to serve you better. If there are any additional insights or details you'd like to share or if you have any questions, please feel free to contact us at drdesilva@ktdoctor.com. Your input is greatly appreciated.",
            "Dear [Name], your review is appreciated, and we're pleased to have had the opportunity to serve your child's healthcare needs. We're always looking for ways to enhance our services, and your feedback is instrumental in this process. If there are any specific aspects of your child's visit that you'd like to discuss further or any suggestions you may have, please reach out to us at drdesilva@ktdoctor.com. Thank you for choosing us for your child's healthcare."]

Neg_Wait_Time = ["I apologize for any inconvenience you experienced due to wait times during your visit. Your time is valuable to us, and we're committed to improving our efficiency. To help us serve you better, please consider using our online portal at ktdoctor.com for scheduling or secure messaging with your doctor. Thank you for your feedback; it helps us make positive changes.",
                 "I'm genuinely sorry for the wait time you encountered [Name]. Your time is important to us, and we understand how frustrating long waits can be. We're actively working on streamlining our processes. If you have any further concerns or would like to provide additional feedback, please reach out to us at drdesilva@ktdoctor.com. We appreciate your patience and feedback.",
                 "I regret that your visit was marred by extended wait times. We're taking your feedback seriously and are dedicated to improving our services. To minimize wait times, you can self-schedule appointments or securely message your doctor through our website at ktdoctor.com. Thank you for bringing this to our attention; it helps us make necessary improvements.",
                 "I'm deeply sorry for any inconvenience caused by wait times during your visit. We understand how valuable your time is, and we're actively working to reduce wait times. For immediate assistance and to provide us with more insights into your experience, please email us at drdesilva@ktdoctor.com. Your feedback is crucial in helping us enhance our service quality."]


# In[10]:


def check_word(string, word):
    if word in string:
        return True
    else:
        return False


# In[11]:


responses = []
names = []
ratings = []
review_texts = []
clinics = []
bus_ids = []

for index, row in yelp_reviews.iterrows():
    
    #RNG
    rand_9 = random.randint(0, 8)
    rand_5 = random.randint(0, 4)
    rand_4 = random.randint(0, 3)
    
    review = pd.DataFrame(row).T
    already_replied = review['already_replied'].values[0]
    date = pd.to_datetime(review['date'].values[0])
    bus_id = review['business_id'].values[0]
    name = review['names'].values[0]
    rating = review['rating'].values[0]
    r_id = review['review_id'].values[0]
    text = review['review_text_org'].values[0]
    clinic = review['Clinic'].values[0]

    if (already_replied == 0) and (date > three_months_ago):
        wait_time = check_word(text, "wait")
        if (rating == 5):
            response = Five_Star[rand_9].replace("[Name]", re.sub(r'\s[A-Za-z]\.\s*', '', name))
        elif (rating == 1):
            if (wait_time == True):
                response = Neg_Wait_Time[rand_4].replace("[Name]", re.sub(r'\s[A-Za-z]\.\s*', '', name))
            else: 
                response = One_Star[rand_5].replace("[Name]", re.sub(r'\s[A-Za-z]\.\s*', '', name))
                
        elif (rating == 2):
            if (wait_time == True):
                response = Neg_Wait_Time[rand_4].replace("[Name]", re.sub(r'\s[A-Za-z]\.\s*', '', name))
            else: 
                response = One_Star[rand_5].replace("[Name]", re.sub(r'\s[A-Za-z]\.\s*', '', name))
                
        elif (rating == 3):
            if (wait_time == True):
                response = Neg_Wait_Time[rand_4].replace("[Name]", re.sub(r'\s[A-Za-z]\.\s*', '', name))
            else: 
                response = T_F_Star[rand_4].replace("[Name]", re.sub(r'\s[A-Za-z]\.\s*', '', name))
            
        elif (rating == 4):
            response = T_F_Star[rand_4].replace("[Name]", re.sub(r'\s[A-Za-z]\.\s*', '', name))
           
        responses.append(response)  # Append the response to the list
        
        names.append(name)
        review_texts.append(text)
        ratings.append(rating)
        clinics.append(clinic)
        bus_ids.append(bus_id)

# Create the DataFrame after processing all reviews
data = {
    "names": names,
    "rating": ratings,
    "review_text": review_texts,
    "clinic": clinics,
    "bus_id": bus_ids,
    "responses": responses
}
Reviews_Responded = pd.DataFrame(data)


# ## Email Summary

# In[13]:


grouped_reviews = {}
email_text = "Good Morning, \n \nHere are all of the Yelp Reviews in the last 3 months that have not been responded to yet: \n\n"

# Create a dictionary to store clinic URLs
clinic_urls = {}

for index, row in Reviews_Responded.iterrows():
    clinic = row['clinic']
    name = row['names']
    rating = int(row['rating'])
    review_text = row['review_text']
    response = row['responses']

    # Check if the clinic is not already in grouped_reviews and initialize it if not
    if clinic not in grouped_reviews:
        grouped_reviews[clinic] = []

    # Add the clinic's URL to the clinic_urls dictionary if it hasn't been added already
    if clinic not in clinic_urls:
        clinic_urls[clinic] = f"https://www.yelp.com/biz/{bus_id}?sort_by=date_desc"

    review_info = {
        'name': name,
        'rating': f'{rating} Star(s)',
        'review_text': review_text,
        'response': response
    }

    grouped_reviews[clinic].append(review_info)

for clinic, reviews in grouped_reviews.items():
    email_text += ('\n----------------------------------------------------------------------------------------------------------- \n\n' + clinic + ':')

    # Add the clinic's URL to the email_text
    email_text += f"\n{clinic_urls[clinic]}"

    for review in reviews:
        # Add review details to the email text
        email_text += (f"\n* {review['name']}: {review['rating']}:\n")
        email_text += (f"{review['review_text']}\n")
        email_text += (f"\nSuggested Response: {review['response']}\n\n")


# In[15]:


mailjet = Client(auth=(MAIL_API_KEY, MAIL_API_KEY_S), version='v3.1')

# Create a list of email addresses
recipient_emails = [email_1, email_2]

# Create a list of recipient dictionaries
recipients = [{'Email': email} for email in recipient_emails]

data = {
    'Messages': [
        {
            'From': {
                'Email': 'kt.yelp.bot@gmail.com',
                'Name': 'Kids & Teens Yelp Bot',
            },
            'To': recipients,
            'Subject': 'Kids & Teens Yelp Reviews',
            'TextPart': email_text,
        },
    ],
}

result = mailjet.send.create(data=data)


# In[16]:


print (result.status_code)

