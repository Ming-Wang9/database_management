
"""
FILE: skeleton_parser.py
------------------
Author: Firas Abuzaid (fabuzaid@stanford.edu)
Author: Perth Charernwattanagul (puch@stanford.edu)
Modified: 04/21/2014

Skeleton parser for CS564 programming project 1. Has useful imports and
functions for parsing, including:

1) Directory handling -- the parser takes a list of eBay json files
and opens each file inside of a loop. You just need to fill in the rest.
2) Dollar value conversions -- the json files store dollar value amounts in
a string like $3,453.23 -- we provide a function to convert it to a string
like XXXXX.xx.
3) Date/time conversions -- the json files store dates/ times in the form
Mon-DD-YY HH:MM:SS -- we wrote a function (transformDttm) that converts to the
for YYYY-MM-DD HH:MM:SS, which will sort chronologically in SQL.

Your job is to implement the parseJson function, which is invoked on each file by
the main function. We create the initial Python dictionary object of items for
you; the rest is up to you!
Happy parsing!
"""

import sys
from json import loads
from re import sub
import re

columnSeparator = "|"

# Dictionary of months used for date transformation
MONTHS = {'Jan':'01','Feb':'02','Mar':'03','Apr':'04','May':'05','Jun':'06',\
        'Jul':'07','Aug':'08','Sep':'09','Oct':'10','Nov':'11','Dec':'12'}

"""
Returns true if a file ends in .json
"""
def isJson(f):
    return len(f) > 5 and f[-5:] == '.json'

"""
Converts month to a number, e.g. 'Dec' to '12'
"""
def transformMonth(mon):
    if mon in MONTHS:
        return MONTHS[mon]
    else:
        return mon

"""
Transforms a timestamp from Mon-DD-YY HH:MM:SS to YYYY-MM-DD HH:MM:SS
"""
def transformDttm(dttm):
    dttm = dttm.strip().split(' ')
    dt = dttm[0].split('-')
    date = '20' + dt[2] + '-'
    date += transformMonth(dt[0]) + '-' + dt[1]
    return date + ' ' + dttm[1]

"""
Transform a dollar value amount from a string like $3,453.23 to XXXXX.xx
"""

def transformDollar(money):
    if money == None or len(money) == 0:
        return money
    return sub(r'[^\d.]', '', money)

def remove_quote(str):
    if str is None:
        return str
    return str.replace('"', '')

def clean_cat(cat_str):
    # category = category.strip()  # remove leading/trailing spaces first
    cat_str = re.sub(r'[^a-zA-Z0-9 ]', '', cat_str)  # Remove any character that's not alphanumeric or a space
    # category = re.sub(r'\s+', ' ', category) # multiple spaces with a single space
    cat_str = re.sub(r'\s+', ' ', cat_str)  # replace multiple spaces with a single space
    return cat_str

"""
Parses a single json file. Currently, there's a loop that iterates over each
item in the data set. Your job is to extend this functionality to create all
of the necessary SQL tables for your database.
"""
def parseJson(json_file):
    with open(json_file, 'r') as f:
        items = loads(f.read())['Items'] # creates a Python dictionary of Items for the supplied json file
        
    # have these files outside of the loop
    with open('items.dat', 'a') as items_f, \
            open ('categories.dat','a') as categories_f, \
            open('users.dat', 'a') as user_f, \
            open('bids.dat', 'a') as bids_f:
            
        user_seen = {}
        # using set to avoid the duplication
        unique_item = set()
        unique_categories = set()
        unique_bids = set()

        for item in items:
            """
            TODO: traverse the items dictionary to extract information from the
            given `json_file' and generate the necessary .dat files to generate
            the SQL tables based on your relation design
            """
            # pass
            # Items
            if item['ItemID'] in unique_item:
                continue
            items_f.write(
                f"{item['ItemID']}|"
                f"{remove_quote(item['Name'])}|"
                f"{transformDollar(item['Currently'].replace('|',''))}|"
                f"{transformDollar(item.get('Buy_Price', 'NULL')).replace('|', '')}|"
                f"{transformDollar(item['First_Bid'].replace('|', ''))}|"
                f"{item['Number_of_Bids']}|"
                f"{transformDttm(item['Started'])}|"
                f"{transformDttm(item['Ends'])}|"
                f"{item['Seller']['UserID']}|"
                f"{remove_quote(item.get('Description', ''))}\n"
            )
            unique_item.add(item['ItemID'])

            #categories
            for category in item['Category']:
                clean_category = clean_cat(category)
                if clean_category: # in case empty strings, skip it
                    cat = (item['ItemID'], category)  # Create a tuple, one item may have multiple categories, so composite primary key 

                    if cat not in unique_categories:
                        categories_f.write(f"{item['ItemID']}|{clean_category}\n")
                        unique_categories.add(cat)

            #seller
            seller = item['Seller']
            seller_id = item['Seller']['UserID']
            seller_rating = seller['Rating']
            seller_location = item.get('Location', '').replace('"', '')
            seller_country = item.get('Country', '')

            # debug for missing location and countries
            # if not seller_location or not seller_country:
            #     print(f"Skipping seller {seller_id}: Missing Seller's Location/Country")
            #     continue

            if seller_id not in user_seen:
                user_seen[seller_id] = (seller_rating, seller_location, seller_country)
            # elif not user_seen[seller_id][1] is None and seller_location:  # if previous location was empty and new one isn't
            #     user_seen[seller_id] = (seller['Rating'], seller_location, seller_country)
                
                
                #user_f.write(f"{seller_id}|{seller['Rating']}|{item.get('Location', '')}|{item['Country']}\n")

            #bids and bidder inside
            if 'Bids' in item and item['Bids']:
                for bid_product in item['Bids']:
                    bid_data = bid_product['Bid']
                    bidder_id = bid_data['Bidder']['UserID']
                    bidder_rating = bid_data['Bidder']['Rating']
                    bidder_location = bid_data['Bidder'].get('Location', '').replace('"', '') # using get(), beacuse location might be null, retun null
                    bidder_country = bid_data['Bidder'].get('Country', '')
                    bid_time = transformDttm(bid_data['Time'])
                    bid_amount = transformDollar(bid_data['Amount'])

                    bid_info = (item['ItemID'], bidder_id, bid_time)
                    if bid_info not in unique_bids:
                        # if not bidder_location or not bidder_country:
                        #     print(f"Skipping Bidder {bidder_id}: Missing Bidder's Location {bidder_location} / Country {bidder_country}")
                        if bidder_id not in user_seen or not bidder_location or not bidder_country:
                            user_seen[bidder_id] = (bidder_rating, bidder_location, bidder_country)
                       
                            #user_f.write(f"{bidder_id}|{bidder_rating}|{escape_quotes(bidder_location)}|{bidder_country}\n")
                        unique_bids.add(bid_info)
                        bids_f.write(f"{bid_time}|{bid_amount}|{item['ItemID']}|{bidder_id}\n")
       
        #avoid write into the file multiple times, write in only one-times and this can avoid duplication???
        for user_id, (rating, location, country) in user_seen.items():
            user_f.write(f"{user_id}|{rating}|{location}|{country}\n")

"""
Loops through each json files provided on the command line and passes each file
to the parser
""" 
def main(argv):
    if len(argv) < 2:
        print >> sys.stderr, 'Usage: python skeleton_json_parser.py <path to json files>'
        sys.exit(1)
    # loops over all .json files in the argument
    for f in argv[1:]:
        if isJson(f):
            parseJson(f)
            print ("Success parsing " + f)

if __name__ == '__main__':
    main(sys.argv)