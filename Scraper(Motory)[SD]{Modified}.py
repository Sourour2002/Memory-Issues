from memory_profiler import profile
from supabase import create_client
from bs4 import BeautifulSoup
import psycopg2.extras
import psycopg2
import cloudscraper
import traceback
import time
import re


# Function that gets the max number of pages amd returns an int value
@profile
def get_max(*args, **kwargs):
    link = "https://ksa.motory.com/en/cars-for-sale/?sort=-published_at&page=1000"
    scraper = cloudscraper.create_scraper()
    data = scraper.get(link, timeout=120)
    soup = BeautifulSoup(data.content, "html.parser")
    
    page_max = soup.find("div", {"class" : "item active"}).find('a').text
    return int(page_max)

# Function that gets the links in each page and returns a list of them 
@profile
def get_links(page_num):
    scraper = cloudscraper.create_scraper()
    data = scraper.get(f"https://ksa.motory.com/en/cars-for-sale/?sort=-published_at&page={page_num}", timeout=120)
    soup = BeautifulSoup(data.content, "html.parser")
    temp_links = soup.find_all("div", {"class" : "title clamp clamp-2"})
    links = []
    for temp_link in temp_links:
        try:
            links.append(temp_link.find("a", href=True)["href"])
        except:
            pass
    return links

# Function that gets data from a link and returns a Dict/JSON variable 
@profile
def get_data(art_link):
    scraper = cloudscraper.create_scraper()
    data = scraper.get(art_link, timeout=120)
    soup = BeautifulSoup(data.content, "html.parser")
    
    print(f"Scraping: {art_link}")
    
    global skip
    
    check = soup.find_all("div", {"class" : "specification-spec"})
    if check != []:
        skip = True
        return 
    
    try:
        art_id = soup.find("h1", {"class" : "vehicles-detail-title cmb-16 cmt-12"}).text.strip().split('\n')[1]
        art_id = re.findall(r"\d+", art_id)[0]
    except:
        art_id = art_link.split("/")[-2]   
    
    art_title = soup.find("h1", {"class" : "vehicles-detail-title cmb-16 cmt-12"}).text.strip().split('\n')[0][:-1].strip()

    art_price = soup.find("div", {"class" : "price-container"}).find_all("div")[-1].text.strip()
    try:
        art_price = re.findall(r"\d+,\d+", art_price)[0]
    except:
        pass

    try:
        art_monthly_payment = soup.find("div", {"class" : "finance-container"}).find("span", {"class" : "value"}).text
    except:
        art_monthly_payment = None
    
    try:
        art_num = soup.find("span", {"class" : "show-phone-number font-weight-bold font-size-14 font-size-md-16 text-color-green"})["data-phone-number"]
    except:
        try:
            art_num = soup.find("span", {"class" : "show-phone-number"})["data-phone-number"]
        except:
            art_num = None

    art_brand = soup.find_all("span", {"class" : "vehicles-detail-overview-item-content"})[0].text.strip().lower()
    art_model = soup.find_all("span", {"class" : "vehicles-detail-overview-item-content"})[1].text.strip().lower()
    art_model_year = soup.find_all("span", {"class" : "vehicles-detail-overview-item-content"})[2].text.strip()
    art_loc = soup.find_all("span", {"class" : "vehicles-detail-overview-item-content"})[3].text.strip()
    art_cond = soup.find_all("span", {"class" : "vehicles-detail-overview-item-content"})[4].text.strip()
    art_body_type = soup.find_all("span", {"class" : "vehicles-detail-overview-item-content"})[5].text.strip()
    art_tra = soup.find_all("span", {"class" : "vehicles-detail-overview-item-content"})[6].text.strip()
    art_color = soup.find_all("span", {"class" : "vehicles-detail-overview-item-content"})[7].text.strip()
    art_km = soup.find_all("span", {"class" : "vehicles-detail-overview-item-content"})[8].text.strip().replace(" ", "")[:-len("km")]
    
    # Getting rid of arabic descriptions
    # arabic_pattern  = re.compile(r'[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]+', re.UNICODE)
    art_desc = soup.find("div", {"class" : "intro-description clamp clamp-3"}).text.strip()[:-len("Read less")]

    # if bool(arabic_pattern.search(art_desc)):
    #     art_desc = None

    # Extracting images
    art_images = ""
    art_images_raw = soup.find_all("div", {"class" : "image"})
    for art_image_raw in art_images_raw:
        art_image = art_image_raw.find("a")["href"]
        art_images = art_images + f"{art_image}| "
    
    art_images = art_images[:len(art_images)-2]

    data = {
        "article_link" : art_link,
        "vehicle_id" : art_id,
        "article_title" : art_title,
        "price" : art_price,
        "monthly_price" : art_monthly_payment,
        "phone" : art_num,
        "brand" : art_brand,
        "model" : art_model,
        "model_year" : art_model_year,
        "location" : art_loc,
        "condition" : art_cond,
        "body_type" : art_body_type,
        "transmission" : art_tra,
        "color" : art_color,
        "mileage" : art_km,
        "description" : art_desc,
        "images" : art_images
    }
    
    return data

# Retry failed connections function
@profile
def retry(func_name, arg=None):
    while True:
        try:
            func_output = func_name(arg)
            return func_output
        except Exception as e:
            #print("Retrying")
            #print(traceback.print_exc())
            continue 

# Variables declarations 
SUPABASE_URL = "https://kciyolpfzmxlkrbcyflz.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImtjaXlvbHBmem14bGtyYmN5Zmx6Iiwicm9sZSI6ImFub24iLCJpYXQiOjE2OTc2NDkxNjksImV4cCI6MjAxMzIyNTE2OX0.M2JLV11RE7pgo7jxal_EyNjXIPl0lrmCWLw-KsOR230"
TEST_SUPABASE_URL="https://izkxcpylzhrcvmqcoszx.supabase.co"
TEST_SUPABASE_KEY="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Iml6a3hjcHlsemhyY3ZtcWNvc3p4Iiwicm9sZSI6ImFub24iLCJpYXQiOjE2OTgzNDI5NjcsImV4cCI6MjAxMzkxODk2N30.KCtYlCqJ_xGuS0spHaphLywFnEWxrjnZ0OwhORMfuAk"
supabase = create_client(TEST_SUPABASE_URL, TEST_SUPABASE_KEY)
skip = False

test_connection_info = {
    "hostname" : "db.izkxcpylzhrcvmqcoszx.supabase.co",
    "database" : "postgres",
    "username" : "postgres",
    "password" : "BigAdsData2023",
    "port" : "5432"
}

connection_info = {
    "hostname" : "http://db.kciyolpfzmxlkrbcyflz.supabase.co",
    "database" : "postgres",
    "username" : "postgres",
    "password" : "u39xnprYQBzk3j7P",
    "port" : "5432"
}

# Function that executes any script to the SQL database
@profile
def postprocessing():
    while True:
        try:
            with psycopg2.connect(
                host = test_connection_info["hostname"],
                dbname = test_connection_info["database"],
                user = test_connection_info["username"],
                password = test_connection_info["password"],
                port = test_connection_info["port"]) as conn:

                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    # Moving all the data from to the buffer table
                    cur.execute(f"SELECT * FROM cars_raw_listing")
                    rows = cur.fetchall()

                    # Insert the fetched data into the destination table
                    for row in rows:
                        cur.execute("INSERT INTO cars_listing_buffer (id, created_at, vehicle_id, article_title, article_link, price, phone, whatsapp, seller_name, seller_location, updated_at, brand, model, model_year, location, condition, service_history, drive_type, transmission, engine_capacity, no_cylinder, no_door, no_seat, accident_history, color, mileage, after_market_mods, features, description, images, monthly_price, regional_specs, no_previous_owner, fuel_type, body_type, featured) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                                    (row["id"], row["created_at"], row["vehicle_id"], row["article_title"], row["article_link"], row["price"], row["phone"], row["whatsapp"], row["seller_name"], row["seller_location"], row["updated_at"], row["brand"], row["model"], row["model_year"], row["location"], row["condition"], row["service_history"], row["drive_type"], row["transmission"], row["engine_capacity"], row["no_cylinder"], row["no_door"], row["no_seat"], row["accident_history"], row["color"], row["mileage"], row["after_market_mods"], row["features"], row["description"], row["images"], row["monthly_price"], row["regional_specs"], row["no_previous_owner"], row["fuel_type"], row["body_type"], row["featured"]))                      
                    
                    # Fetch unique values from cars_listing_buffer
                    cur.execute("SELECT DISTINCT brand FROM cars_listing_buffer")
                    unique_brands = cur.fetchall()                      
                    
                    for brand in unique_brands:
                        # Check if the brand already exists in cars_brand
                        cur.execute("SELECT * FROM cars_brand WHERE title = %s", (brand[0],))
                        exists = cur.fetchone()
                    
                        if not exists:
                            # If it doesn't exist, insert it into cars_brand
                            cur.execute("INSERT INTO cars_brand (title) VALUES (%s)", (brand[0],))
                    print("Table Populated: cars_brand")
                    
                    # Fetch unique brand and model combinations from cars_listing_buffer
                    cur.execute("SELECT DISTINCT brand, model FROM cars_listing_buffer")
                    unique_models = cur.fetchall()

                    for brand, model in unique_models:
                        # Find the associated brand_id from cars_brand
                        cur.execute("SELECT id FROM cars_brand WHERE title = %s", (brand,))
                        brand_id = cur.fetchone()

                        if brand_id:
                            # Insert the data into cars_model
                            cur.execute("INSERT INTO cars_model (title, brand_id) VALUES (%s, %s)", (model, brand_id[0]))
                    print("Table Populated: cars_model")
                            
                    # Fetch data from cars_listing_buffer
                    cur.execute("SELECT * FROM cars_listing_buffer")
                    raw_data = cur.fetchall()

                    for row in raw_data:
                        # Find the corresponding brand_id from cars_brand
                        cur.execute("SELECT id FROM cars_brand WHERE title = %s", (row["brand"],))
                        brand_id = cur.fetchone()

                        # Find the corresponding model_id from cars_model
                        cur.execute("SELECT id FROM cars_model WHERE title = %s", (row["model"],))
                        model_id = cur.fetchone()

                        if brand_id and model_id:
                            # Insert the row into cars_listing
                            cur.execute("INSERT INTO cars_listing (id, created_at, vehicle_id, article_title, article_link, price, phone, whatsapp, seller_name, seller_location, updated_at, brand_id, model_id, model_year, location, condition, service_history, drive_type, transmission, engine_capacity, no_cylinder, no_door, no_seat, accident_history, color, mileage, after_market_mods, features, description, images, monthly_price, regional_specs, no_previous_owner, fuel_type, body_type, featured) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                                        (row["id"], row["created_at"], row["vehicle_id"], row["article_title"], row["article_link"], row["price"], row["phone"], row["whatsapp"], row["seller_name"], row["seller_location"], row["updated_at"], brand_id[0], model_id[0], row["model_year"], row["location"], row["condition"], row["service_history"], row["drive_type"], row["transmission"], row["engine_capacity"], row["no_cylinder"], row["no_door"], row["no_seat"], row["accident_history"], row["color"], row["mileage"], row["after_market_mods"], row["features"], row["description"], row["images"], row["monthly_price"], row["regional_specs"], row["no_previous_owner"], row["fuel_type"], row["body_type"], row["featured"]))
                    print("Table Populated: cars_listing")
                    
                    # Deleting all buffer entires
                    cur.execute(f"DELETE FROM cars_listing_buffer")

        except:
            print(traceback.print_exc())
            conn.rollback()
            continue
        finally:
            if conn is not None:
                conn.close()
            return None

# Main scraping program function
@profile
def main():
    page_max = retry(get_max)
    print(f"Max page is {page_max}")
    
    for page_num in range(1, 5):
        link_list = retry(get_links, page_num)
        print(f"Scraping Page {page_num}")
        
        existing_articles_no = 0
        for link in link_list:
            global skip
            skip = False
            
            # Speed tuning
            time.sleep(10)
            
            while True:
                try:
                    response = supabase.table("cars_raw_listing").select("article_link").eq("article_link", link).execute()
                    break
                except:
                    print(traceback.print_exc())
                    print("Connection Failed, Retrying...")
                    
            try:
                response.data[0]["article_link"]
                existing_articles_no += 1
                continue
            except:
                pass
            
            art_data = retry(get_data, link)
            
            if skip:
                continue
                
            while True:
                try:
                    supabase.table("cars_raw_listing").insert(art_data).execute()
                    break
                except:
                    print(traceback.print_exc())
                    print("Connection Failed, Retrying...")
                    
        if existing_articles_no >= 10:
            return

# Main script loop
while True:
    # Running main script Function
    main()
    break
    # Read the current status from the file
    with open("flags.txt", 'r') as file:
        content = file.readlines()

    # Check if the status is True or False
    postprocessor_running = "True" in content[0]

    # Checking loop
    while True:
        if postprocessor_running:
            print("Waiting for PostProcessor...")
            time.sleep(60)
            
            with open("flags.txt", 'r') as file:
                content = file.readlines()
            postprocessor_running = "True" in content[0]
        else:
            with open("flags.txt", 'r') as file:
                content = file.readlines()
            content[0] = "PostProcessor running: True\n"
            with open("flags.txt", 'w') as file:
                file.writelines(content)
            
            print("Running PostProcessor...")
            postprocessing()
            
            with open("flags.txt", 'r') as file:
                content = file.readlines()
            content[0] = "PostProcessor running: False\n"
            with open("flags.txt", 'w') as file:
                file.writelines(content)        
            break
    
    print("Sleeping for 30 Minutes...")
    time.sleep(1800)