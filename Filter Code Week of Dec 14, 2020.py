#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np


# # INPUTS: 
# Input the values you would like to filter by:

# Category Mappings (to convert word-cats into numbers)
# 
# mapping_1 = products_static[["category_one", "category_one_en"]].drop_duplicates()
# mapping_1.set_index("category_one").dropna().sort_index()
# 
# mapping_2 = products_static[["category_two", "category_two_en"]].drop_duplicates()
# mapping_2.set_index("category_two").dropna().sort_index()
# 
# mapping_3 = products_static[["category_three", "category_three_en"]].drop_duplicates()
# mapping_3.set_index("category_three").dropna().sort_index()

# In[29]:


#Step 1: Cat Filter
cat_lst_1 = [47, 49, 1017]
cat_lst_2 = [875, 887, 1117]
cat_lst_3 = [8669, 8637, 9189, 9266, 10723, 21419, 10710]

#Step 2 & 2.5: Time Filter
min_years = 1
updated_previously_weeks = 2

#Step 4: Brand Total GMV Filter
min_gmv = 1000

#Step 5: Brand Avg Price Filter
min_unit_price = 100

#Step 6: GMV Conc. Filter
top_n = 3 #concetration num of products
min_concentration = 0.3 

#Step 7: Avg Product Rating Filter
min_avg_star = 4.5

#Step 8: Bad Customer Ratings Filter
max_bad_rating = 0.10


# # Step 0: Import Data

# In[3]:


shops_gmv = pd.read_csv("shops_gmv_data.csv")
products_static = pd.read_csv("products_static_1607946056.csv")

#use the timestamp from products instead ^^ that will be time now. 
models_static = pd.read_csv("models_static_data.csv")


# Change data types in dataframes to conserve memory:

# In[4]:


def convert_to_cats(df, to_convert):
    '''
    Converts cols in to_convert_list to categories. 
    
    Inputs:
        df: a DataFrame
        to_convert: a list of column names (as strings)
                    we'd like to convert
    Returns:
        Nothing; modifies dataframe in place
    '''
    for col in df.columns:
        if col in to_convert:
            df[col] = df[col].astype("category")


# In[5]:


to_convert_models = ['category_one', 'category_one_en',
                     'category_one_th', 'category_two',
                     'category_two_en', 'category_two_th',
                     'category_three', 'category_three_en',
                     'category_three_th']
convert_to_cats(models_static, to_convert_models)

to_convert_products = ['category_one', 'category_one_en',
                      'category_one_th', 'category_two', 
                      'category_two_en', 'category_two_th',
                      'category_three', 'category_three_en', 
                      'category_three_th', 'reviews_count_context', 
                      'reviews_count_image', 'shopee_verified', 
                      'show_discount']
convert_to_cats(products_static, to_convert_products)

to_convert_shops = ["shopid", "category_one", "category_two",
                    "category_three"]
convert_to_cats(shops_gmv, to_convert_shops)


# In[6]:


def downcast_numbers(df):
    '''
    Downcasts floats and ints.
    
    Inputs:
        df: a DataFrame objecet. 
    Returns:
        Nothing; modifies list in place
    '''
    for col in df.columns:
        if df[col].dtype == "float":
            df[col] = pd.to_numeric(df[col], downcast="float")
        if df[col].dtype == "int":
            df[col] = pd.to_numeric(df[col], downcast="unsigned")


# In[7]:


downcast_numbers(models_static)
downcast_numbers(products_static)
downcast_numbers(shops_gmv)


# # Step 1: Category Filter

# Filter our DataFrames to only keep rows with Categories we care for. 
# 

# In[8]:


def filter_by_category(df, category_n, ok_cat_lst):
    '''
    Keeps rows in the df that have cats in ok_cat_lst. 
    
    Inputs:
        df: a DataFrame
        category_n: (str) eg. "category_one"
        ok_cat_lst: (lst) of acceptable cat for that 
          cat as numbers eg [50, 26]
    Returns:
        Nothing; modifies list in place
    '''
    
    mask = df[category_n].isin(ok_cat_lst)
    df = df[mask]


# In[9]:


def filter_by_category(df, category_n, ok_cat_lst):
    '''
    Keeps rows in the df that have cats in ok_cat_lst. 
    
    Inputs:
        df: a DataFrame
        category_n: (str) eg. "category_one"
        ok_cat_lst: (lst) of acceptable cat for that 
          cat as numbers eg [50, 26]
    Returns:
        The modified df 
    '''
    
    mask = df[category_n].isin(ok_cat_lst)
    return df[mask]


# In[10]:


models_static = filter_by_category(models_static, "category_one", cat_lst_1)
products_static = filter_by_category(products_static, "category_one", cat_lst_1) 


# In[11]:


models_static = filter_by_category(models_static, "category_two", cat_lst_2)
products_static = filter_by_category(products_static, "category_two", cat_lst_2) 


# In[12]:


models_static = filter_by_category(models_static, "category_three", cat_lst_3)
products_static = filter_by_category(products_static, "category_three", cat_lst_3) 


# # Step 2: Keep only Established Products 

# Filter out products that have been sold for less than a specified time. 

# In[13]:


from datetime import datetime
import math 

def filter_out_new_products(df, min_years):
    '''
    Filters out rows from our products_static df that 
    have been sold less than a specified time
    '''
    
    unix_time_now = math.ceil(datetime.now().timestamp()) #CHANGE THIS AT TOP BASED ON NAME OF PRODUCTS CSV
    
    min_s = min_years * 365 * 24 * 60 * 60 
    
    mask = (unix_time_now - products_static["ctime"] > min_s)
    
    return df[mask]


# In[14]:


products_static = filter_out_new_products(products_static, min_years)


# # Step 2 1/2: Keep only Products that Sellers are Updating

# Filter out products that have not been updated recently within some specified time

# In[15]:


def filter_out_unupdated_products(df, updated_previously_weeks):
    '''
    Filters out rows from our products_static df that 
    have been sold less than a specified time
    '''
    
    unix_time_now = math.ceil(datetime.now().timestamp())
    
    max_s = updated_previously_weeks * 7 * 24 * 60 * 60
    
    mask = (unix_time_now - products_static["modified_at"] < max_s)
    
    return df[mask]


# In[16]:


products_static = filter_out_unupdated_products(products_static, updated_previously_weeks)


# # Step 3: Brand Definition

# Extract unique brand names from the remaining products

# In[17]:


mask = products_static["brand"] != "No Brand(ไม่มียี่ห้อ)"
products_static = products_static[mask]

brands_list = products_static["brand"].unique()
brands_list = np.delete(brands_list, np.where(brands_list == ('No Brand(ไม่มียี่ห้อ)')))


# In[18]:


#Clean up brands_df
brands_df = pd.DataFrame(brands_list, columns = ["Brand Name"])

brands_df.dropna(inplace=True)

s1 = brands_df["Brand Name"] != "None"
brands_df = brands_df[s1]

s2 = brands_df["Brand Name"] != "0"
brands_df = brands_df[s2] 


# In[19]:


#Set up columns for our brands_df
brands_df["Brand_GMV"] = 0
brands_df["Average_Unit_Price"] = 0
brands_df["GMV_Concentration"] = 0
brands_df["Weighted_Star_Rating"] = 0
brands_df["Bad_Rating_Percent"] = 0
brands_df.set_index("Brand Name", inplace = True)


# In[20]:


#Make new column in products_static for GMV of a product (price * sold)
products_static["product_gmv"] = products_static["price"] * products_static["sold"]
products_static["weighted_star"] = products_static["rating_star"] * products_static["sold"]


# In[21]:


for brand in brands_df.index:
    
    #make a sub-df containing only rows with the correct brand
    my_brand_df = products_static[products_static["brand"] == brand]
    my_brand_df = my_brand_df.sort_values(by=["product_gmv"], ascending=False) 
    
    #gmv calculation
    brand_gmv = my_brand_df["product_gmv"].sum()
    brands_df.loc[brand, "Brand_GMV"] = brand_gmv
    
    #volume calculation
    brand_volume = my_brand_df["sold"].sum()
    brands_df.loc[brand, "Average_Unit_Price"] = brand_gmv / brand_volume
    
    #GMV Concentration Calculation
    if len(my_brand_df) >= top_n: #in case brand sells less than like 5 products
        top_n_df = my_brand_df.head(top_n)
        top_n_gmv = top_n_df["product_gmv"].sum()
        brands_df.loc[brand, "GMV_Concentration"] = top_n_gmv / brand_gmv
        
    #Weighted Star Rating
    total_star = my_brand_df["weighted_star"].sum()
    brands_df.loc[brand, "Weighted_Star_Rating"] = total_star / brand_volume
    
    #Bad Rating Count
    bad_rating_count = my_brand_df["rating_count_one"].sum() + my_brand_df["rating_count_two"].sum()
    total_rating_count = my_brand_df["rating_count_total"].sum()
    
    brands_df.loc[brand, "Bad_Rating_Percent"] = bad_rating_count / total_rating_count
    


# # Step 4: Brand Total GMV Filter

# In[22]:


brands_df = brands_df[brands_df["Brand_GMV"] >= min_gmv]


# # Step 5: Brand Average Price Filter

# In[23]:


brands_df = brands_df[brands_df["Average_Unit_Price"] >= min_unit_price]


# # Step 6: GMV Concentration Filter

# In[24]:


brands_df = brands_df[brands_df["GMV_Concentration"] >= min_concentration]


# # Step 7: Average Product Rating Filter

# In[25]:


brands_df = brands_df[brands_df["Weighted_Star_Rating"] >= min_avg_star]


# # Step 8: Bad Customer Ratings Filter

# In[27]:


brands_df = brands_df[brands_df["Bad_Rating_Percent"] <= max_bad_rating]


# # (Step 9: GMV Growth?)

# In[ ]:





# # Output: Brands that Meet Our Criterion

# In[28]:


brands_df

