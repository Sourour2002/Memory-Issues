from supabase import create_client
from bs4 import BeautifulSoup
import psycopg2.extras
import psycopg2
import cloudscraper
import traceback
import time
import re
import gc

# Function that gets the max number of pages amd returns an int value
def get_max(*args, **kwargs):
    link = "https://ksa.yallamotor.com/used-cars/search?page=1&sort=updated_desc"
    scraper = cloudscraper.create_scraper()
    data = scraper.get(link, timeout=120)
    soup = BeautifulSoup(data.content, "html.parser")
    
    num_of_pages = soup.find("div", {"class" : "pagination m32t"}).find_all("a")
    maxpage = 0
    for child in num_of_pages:
        try:
            if maxpage < int(child.text):
                maxpage = int(child.text)
        except:
            pass    
    
    link, scraper, data, soup, num_of_pages, child = None, None, None, None, None, None
    del link, scraper, data, soup, num_of_pages, child
    gc.collect()
    return maxpage

# Function that gets the links in each page and returns a list of them 
def get_links(page_num):
    scraper = cloudscraper.create_scraper()
    data = scraper.get(f"https://ksa.yallamotor.com/used-cars/search?page={page_num}&sort=updated_desc", timeout=120)
    soup = BeautifulSoup(data.content, "html.parser")
    temp_links = soup.find_all("a", {"class" : "black-link"}, href=True)
    links = []
    for temp_link in temp_links:
        temp_link = temp_link["href"]
        links.append(f"https://ksa.yallamotor.com{temp_link}")
    
    scraper, data, soup, temp_links, temp_link = None, None, None, None, None
    del scraper, data, soup, temp_links, temp_link
    gc.collect()
    return links

# Function that gets data from a link and returns a pandas dataframe    
def get_data(art_link):
    scraper = cloudscraper.create_scraper()
    data = scraper.get(art_link, timeout=120)
    soup = BeautifulSoup(data.content, "html.parser")
    
    art_type = art_link.split("/")[3].lower()
    if "used" in art_type:
        art_type = "Used"
    else:
        art_type = "New"
    
    art_title = soup.find("h1", {"class" : "font24"}).text

    print(f"Scraping: {art_link}")
    
    script_tags = soup.find_all("script")
    for script_tag in script_tags:
        try:
            txt = script_tag.decode_contents()
        except:
            continue
        var_re = re.compile(r'var text = \"\+\d+\";')
        try:
            num = var_re.findall(txt)[0]
        except:
            continue
        if num != "":
            art_number = re.findall(r"\+\d+", num)[0]
    try:
        art_number
    except NameError:
        art_number = "NULL"
    
    
    art_price = soup.find("span", {"class" : "font28 font-b"}).text
    
    try:
        temp_art_whatsapp = soup.find("div", {"class" : "p16 p0t"}).find("a", {"id" : "btn_whatsapp"})["href"]
        art_whatsapp = re.findall(r"\+\d+", temp_art_whatsapp)[0]
    except:
        art_whatsapp = None        
    
    try:
        art_seller_name = soup.find("div", {"class" : "seller"}).find("h6", {"class" : "font-b"}).text
        if art_seller_name == "":
            art_seller_name = None
    except:
        art_seller_name = None
    
    try:
        art_seller_location = soup.find("div", {"class" : "seller"}).find("p").text
    except:
        art_seller_location = None
    if art_seller_location is not None:
        if len(art_seller_location) < 1:
            art_seller_location = None
    
    try:
        art_update_date = soup.find("div", {"class" : "position-abs right0 text-right"}).find_all("div")[-1].text
    except:
        art_update_date = None        
    
    art_brand = art_link.split("/")[4].lower()
    art_model = art_link.split("/")[5].lower()
    
    art_details = soup.find_all("div", {"class" : "col is-5 p0 font-b"})
    art_color = soup.find_all("div", {"class" : "font14 text-center font-b m2t"})[5].text
    art_model_year = art_details[0].text
    art_location = art_details[1].text
    art_kilometers = art_details[2].text
    art_engine = art_details[3].text
    art_transmission = art_details[4].text
    art_s_history = art_details[5].text
    art_drive_type = art_details[6].text.strip()
    art_specs = art_details[7].text
    art_num_of_previous_owners = art_details[8].text.strip()
    art_fuel_type = art_details[9].text
    art_num_of_cylinders = art_details[10].text
    art_num_of_doors = art_details[11].text
    art_num_of_seats = art_details[12].text
    art_a_history = art_details[13].text
    art_mods = art_details[14].text
    
    try:
        features = soup.find_all("a", {"class" : "m12l feature-link"})
        if features == []:   
            features = soup.find_all("span", {"class" : "m12l"})
        art_features = ""
        for feature in features:
            feature = feature.decode_contents().strip()
            art_features += f"{feature}, "
        art_features = art_features.rstrip(art_features[-1])
        art_features = art_features.rstrip(art_features[-1])
    except:
        art_features = None        
    
    try:
        art_desc = soup.find("div", {"id" : "whyText"}).text.strip()
    except:
        art_desc = None
    
    art_images = ""
    art_images_raw_temp = soup.find_all("img", {"class" : "img-main"})
    art_images_max = int(len(art_images_raw_temp)/2)
    art_images_raw = art_images_raw_temp[:art_images_max]
    for art_image_raw in art_images_raw:
        art_image = art_image_raw["src"]
        art_images = art_images + f"{art_image}| "
    
    art_images = art_images[:len(art_images)-2]        
    
    try:
        art_featured = soup.find("div", {"class" : "position-abs color-white font14"}).text
        art_featured = "True"
    except:
        art_featured = "FALSE"    
    
    final_data = {
        "article_link" : art_link,
        "condition" : art_type,
        "article_title" : art_title,
        "phone" : art_number,
        "price" : art_price,
        "whatsapp" : art_whatsapp,
        "seller_name" : art_seller_name,
        "seller_location" : art_seller_location,
        "updated_at" : art_update_date,
        "color" : art_color,
        "brand" : art_brand,
        "model" : art_model,
        "model_year" : art_model_year,
        "location" : art_location,
        "mileage" : art_kilometers,
        "engine_capacity" : art_engine,
        "transmission" : art_transmission,
        "service_history" : art_s_history,
        "drive_type" : art_drive_type,
        "regional_specs" : art_specs,
        "no_previous_owner" : art_num_of_previous_owners,
        "fuel_type" : art_fuel_type,
        "no_cylinder" : art_num_of_cylinders,
        "no_door" : art_num_of_doors,
        "no_seat" : art_num_of_seats,
        "accident_history" : art_a_history,
        "after_market_mods" : art_mods,
        "features" : art_features,
        "description" : art_desc,
        "images" : art_images,
        "featured" : art_featured}
    
    soup, data, scraper, art_type, art_title, script_tags, script_tag, txt, var_re, num, art_number, art_price, temp_art_whatsapp, art_whatsapp, art_seller_name, art_seller_location, art_update_date, art_brand, art_link, art_model, art_details, art_color, art_model_year, art_location, art_kilometers, art_engine, art_transmission, art_s_history, art_drive_type, art_specs, art_num_of_previous_owners, art_fuel_type, art_num_of_cylinders, art_num_of_doors, art_num_of_seats, art_a_history, art_mods, features, art_features, feature, art_desc, art_images, art_images_raw_temp, art_images_raw, art_images_max, art_image_raw, art_featured = [None] * 47
    del soup, data, scraper, art_type, art_title, script_tags, script_tag, txt, var_re, num, art_number, art_price, temp_art_whatsapp, art_whatsapp, art_seller_name, art_seller_location, art_update_date, art_brand, art_link, art_model, art_details, art_color, art_model_year, art_location, art_kilometers, art_engine, art_transmission, art_s_history, art_drive_type, art_specs, art_num_of_previous_owners, art_fuel_type, art_num_of_cylinders, art_num_of_doors, art_num_of_seats, art_a_history, art_mods, features, art_features, feature, art_desc, art_images, art_images_raw_temp, art_images_raw, art_images_max, art_image_raw, art_featured
    gc.collect()
    
    return final_data

# Retry failed connections function
def retry(func_name, arg=None):
    while True:
        try:
            func_output = func_name(arg)
            func_name, arg = None, None
            del func_name, arg
            gc.collect()
            return func_output
        except:
            #print("Retrying")
            #print(traceback.print_exc())
            gc.collect()
            continue

# Variables declarations 
SUPABASE_URL = "https://kciyolpfzmxlkrbcyflz.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImtjaXlvbHBmem14bGtyYmN5Zmx6Iiwicm9sZSI6ImFub24iLCJpYXQiOjE2OTc2NDkxNjksImV4cCI6MjAxMzIyNTE2OX0.M2JLV11RE7pgo7jxal_EyNjXIPl0lrmCWLw-KsOR230"
TEST_SUPABASE_URL="https://izkxcpylzhrcvmqcoszx.supabase.co"
TEST_SUPABASE_KEY="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Iml6a3hjcHlsemhyY3ZtcWNvc3p4Iiwicm9sZSI6ImFub24iLCJpYXQiOjE2OTgzNDI5NjcsImV4cCI6MjAxMzkxODk2N30.KCtYlCqJ_xGuS0spHaphLywFnEWxrjnZ0OwhORMfuAk"
supabase = create_client(TEST_SUPABASE_URL, TEST_SUPABASE_KEY)

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
            gc.collect()
            return None


# Main scraping program function
def main():
    page_max = retry(get_max)
    print(f"Max page is {page_max}")
    
    for page_num in range(1, page_max+1):
        link_list = retry(get_links, page_num)
        print(f"Scraping Page {page_num}")
        
        existing_articles_no = 0
        for link in link_list:
            while True:
                try:
                    response = supabase.table("cars_listing").select("article_link, featured").eq("article_link", link).execute()
                    break
                except:
                    print(traceback.print_exc())
                    print("Connection Failed, Retrying...")
            
            try:
                response.data[0]["article_link"]
                if response.data[0]["featured"] == "True":
                    continue
                existing_articles_no += 1
                continue
            except:
                pass
            
            art_data = retry(get_data, link)
                
            while True:
                try:
                    supabase.table("cars_raw_listing").insert(art_data).execute()
                    break
                except:
                    print(traceback.print_exc())
                    print("Connection Failed, Retrying...")
            
            response, art_data = None, None
            del response, art_data
            gc.collect()
            
        if existing_articles_no >= 10:
            return
        
        existing_articles_no, link_list = None, None
        del existing_articles_no, link_list
        gc.collect()
            
# Main script loop
while True:
    # Running main script Function
    main()
    
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