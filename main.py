import json
import requests
import re
from bs4 import BeautifulSoup
import mysql.connector
from datetime import datetime


db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Hoangvu123@',
    'database': 'scraping_db'
}

def insert_into_MySQL(data):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Add thời gian scrape data
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        data_with_time = data + (now,)

        # load data vào MySQL
        insert_query = ("INSERT INTO gpu (ItemId, Title, Brand, Rate, Rate_number, Current_price, Shipping, Total_price, Image_url, Features, Event_time) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        cursor.execute(insert_query, data_with_time)
        connection.commit()

    except mysql.connector.Error as error:
        print(f"Error inserting data into MySQL: {error}")

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def process_GPU_data(soup):
    global hdmi_info, displayport_info
    gpu_blocks = soup.find_all('div', class_='item-cell')
    rows = []
    for gb in gpu_blocks:
        # Lay ID tu id attr cua item-container class
        id = gb.find('div', class_='item-container').get('id')

        # Lay title từ item-title class cua anchor element
        title = gb.find('a', class_='item-title')
        if title:
            title = title.text.strip()

        # Lay brand element từ item-brand class của anchor element
        brand_element = gb.find('div', class_='item-branding')
        brand_names = None
        if brand_element:
            brand_img = brand_element.find('img')
            # Kiếm brand trong alt element của img. Nếu không có thì tách từ title
            if brand_img and 'alt' in brand_img.attrs:
                brand_names = brand_img['alt']
            elif 'Radeon RX' in title:
                brand_names = 'AMD'
            else:
                brand_names = title.split()[0]

        # Lấy rate element từ class item-rating cua anchor element
        rate_element = gb.find('a', class_='item-rating')
        rate = None
        if rate_element:
            # Nếu rate_element tồn tại thì lấy title và remove "Raing +"
            rate = float(rate_element.get('title').replace("Rating + ", ""))

        # Lấy rate tag từ item-rateing-num class của span element
        ratenum_element = gb.find('span', class_='item-rating-num')
        rate_number = None
        if ratenum_element:
            # Nếu tồn tại rate_element thì áp dụng regex để xoá ()
            rate_number = int(re.search(r'\d+',ratenum_element.text).group())

        # Lấy price element từ price-current class của li element
        price_element = gb.find('li', class_='price-current')
        # Kiểm tra GPU nào không có strong và sup ele ở price-current (special price) thì lấy price-was
        if price_element.strong and price_element.sup:
            price_text = price_element.strong.text.replace(",", "") + price_element.sup.text
            price = float(price_text)
        else:
            price_was_element = gb.find('li', class_='price-was')
            if price_was_element:
                pattern = r'\d{1,3}(?:,\d{3})*(?:\.\d+)?'
                match = re.search(pattern,price_was_element.text)
                if match:
                    price = float(match.group().replace(',', ''))
            else:
                price = None

        # Lấy shipping từ price-ship class của li element
        shipping_element = gb.find('li', class_='price-ship')
        shipping = 0
        # Nếu tốn phí shipping thì tách '$' và ' Shipping'
        if shipping_element.text.startswith('$'):
            shipping = float(re.search(r'\d+\.\d+', shipping_element.text).group())

        # Tính tổng giá mua
        total_price = price + shipping

        # Lấy img url từ source attr của 'item-img' class
        image_url = gb.find('a', class_='item-img').img['src']

        # Kiếm list feature trong item-features class
        features_list = gb.find('ul', class_='item-features')
        if features_list:
            features_list = features_list.find_all('li')
            # Tạo variable features, max_resolution, hdmi, display port, card, model
            features = {}
            max_resolution = None
            hdmi = None
            displayport = None
            card = None
            model = None
            # Với mỗi feature thì split và remove white space
            for feature in features_list:
                feature = feature.text
                if feature.startswith('Max Resolution'):
                    max_resolution = feature.split(':')[1].strip()

                if feature.startswith('HDMI'):
                    hdmi = feature.split(':')[1].strip()

                if feature.startswith('DisplayPort'):
                    displayport = feature.split(':')[1].strip()

                if feature.startswith('Card Dimensions'):
                    card = feature.split(':')[1].strip()

                if feature.startswith('Model'):
                    model = feature.split(':')[1].strip()

                # Gom lai trong dictionary
                features = {"MaxResolution": max_resolution,
                            "HDMI": hdmi,
                            "DisplayPort": displayport,
                            "DirectX": card,
                            "Model":model}
            features = json.dumps(features)
        else:
            features = None
            # Gom các cột để insert vào MySQL
        gpu = (id, title, brand_names, rate, rate_number, price, shipping, total_price, image_url, features)

        # Insert data vào MySQL
        insert_into_MySQL(gpu)

def scraping_data(from_, to_):
    for i in range(from_, to_ + 1):
        PAGE = "https://www.newegg.com/GPUs-Video-Graphics-Cards/SubCategory/ID-48/Page-" + str(i)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:98.0) Gecko/20100101 Firefox/98.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0"
            # "cookie": 'NV%5FW57=USA; NV%5FW62=en; NV_TID_LC=7709; NV%5FGAPREVIOUSPAGENAME=home%3AComponents%20%26%20Storage%3AGPUs%20%26%20Video%20Graphics%20Devices%3AGPUs%20%2F%20Video%20Graphics%20Cards%3Asubcat; osano_consentmanager_uuid=4010ff53-1cb3-468b-9bae-bd197d653a07; osano_consentmanager=S3Tnly34L7WwM-BFY7ximY6YTQe1We5QqwfbjJjH4yoRrhdWt9UcMmajOsTjDEJi7-7AbWzf3TN0-VlKfTtout60MFPCjBCDdEDLITUbAd5-HMUVrGaTlOz_mbN50forR0er7WUUMGgY9embV7ZJQI9YVDcKBa_pV93lcnomq8UVeb4ma2mmfiPoHnedLrMPpOWqfcCwL2rv02XI31dL6iplo09WZ_vtRcyVd3guSDRrVikGXipGETZJsfMFP_emNh-63zA7miQi93bA5iG-nwzVCiUSASYL3i_Jbg==; _gcl_au=1.1.516378680.1699508043; _gcl_au=1.1.516378680.1699508043; _gcl_au=1.1.516378680.1699508043; _ga_TR46GG8HLR=GS1.1.1699508045.1.0.1699508045.0.0.0; _ga_TR46GG8HLR=GS1.1.1699508045.1.0.1699508045.0.0.0; __attentive_id=a746a5f4525e4956a891e6f771739fc6; _attn_=eyJ1Ijoie1wiY29cIjoxNjk5NTA4MDQ5MzA3LFwidW9cIjoxNjk5NTA4MDQ5MzA3LFwibWFcIjoyMTkwMCxcImluXCI6ZmFsc2UsXCJ2YWxcIjpcImE3NDZhNWY0NTI1ZTQ5NTZhODkxZTZmNzcxNzM5ZmM2XCJ9In0=; __attentive_cco=1699508049310; cmp_choice=none; _tt_enable_cookie=1; _ttp=-LkOgzTTCh6jtqmho5jV58JJ4B2; __gsas=ID=ca1a8fbcfc393eb7:T=1699512243:RT=1699512243:S=ALNI_MYH_JlUPu7EtHLH5P8nITykiScbWg; xyz_cr_100393_et_137==NaN&cr=100393&wegc=&et=137&ap=; NE_STC_V1=30dfa3db1489abbc0b16b6201d118940b033217c7a83788cccb71f01963fbce3fd85584b; ak_bmsc=5477BD84F666FA702ED974B7E62F9105~000000000000000000000000000000~YAAQvAyrcW852q2LAQAAzZ+RuRWNpJk+rmdDF6lgBnE21ipCtpBh2s1XlisHMxSYxFwJ4xxh4p6lvsaShVFiYihC9TK2DTdWxe+yUTVGHWWsp8GqoqbZIo9r3Em5YDTbgLYjafXG3UHX3IsSjQHL70aDcCFg+fLiCqX4wE4pdfXcPXdb1InIYebHzG4u8+RgzZKKLhqCyjB/XjRtdh19djDPiM0/rKnhUkXaBi3xoNd+9EwIS0KLDiI7lrKuNHbwNqdY3ois6AC3VKsq4CImJzmjGRvtgPSkIK2UWz8m44/P4nPiIwbhuHImozhIxj7D3lOyLM1fPf84gmgd+cyLxe6YXUYQcRJUMlK1KUI8vXV9w1Ztw1cY7ncGNYaCwVVVdyLdNEQ3GDn7; NVTC=248326808.0001.044b7ac79.1699502931.1699625411.1699626893.70; NID=4M9D9D1j340M9D7272; __cf_bm=7hKG4OBPsdO2MO5bcuHjvuRJktGL6flogJ5w_cYR2Fk-1699626892-0-Acb5HGdEWWimv0/WevsiF+neeMQ2vemNAME3KYzyVxuYvP7Wl5AuHtWYKJ71fkaqeYo8BPkCAq8XTplTwfzRJ10=; cf_clearance=XpsxiXDlLJE84bxZtKJKIcjjV7_xR.5QkzvoYf6T5jE-1699626895-0-1-fb1eed0d.8df1c212.ac1fb2ed-0.2.1699626895; __attentive_pv=1; __attentive_ss_referrer=ORGANIC; NV%5FCONFIGURATION=#5%7b%22Sites%22%3a%7b%22USA%22%3a%7b%22Values%22%3a%7b%22w58%22%3a%22USD%22%2c%22wd%22%3a%221%22%2c%22w39%22%3a%227709%22%2c%22w57%22%3a%22USA%22%7d%2c%22Exp%22%3a%221786023295%22%7d%7d%7d; NV%5FDVINFO=#5%7b%22Sites%22%3a%7b%22USA%22%3a%7b%22Values%22%3a%7b%22w19%22%3a%22Y%22%7d%2c%22Exp%22%3a%221699713295%22%7d%7d%7d; _ga=GA1.2.1918245236.1699502935; _gid=GA1.2.1729403555.1699626896; _gat=1; NV_NVTCTIMESTAMP=1699626897; __attentive_dv=1; _uetsid=51d7bcf07fd611eea7f25126fb9fb185; _uetvid=9b7529907ec111ee8c00f34386f5aed3; __gads=ID=6c76a91fb17466d6:T=1699508050:RT=1699626897:S=ALNI_MbxdPD6-UCiIZNORxzCGcUJKcMagg; __gpi=UID=00000c82a3f05d0d:T=1699508050:RT=1699626897:S=ALNI_MZ4EV9cKCGrhrp2_5jZaKNsAp_q2A; _ga_TR46GG8HLR=GS1.1.1699626895.6.0.1699626913.0.0.0; bm_sv=1CBA7A380E8CD89EE06E9BB290045D06~YAAQvAyrcXRf2q2LAQAAmpOouRWIk7Trr3l/z7G5+H0HzRPQovi7wTFZLo/kjY5wnJFhdgznA5NZLUKpaz/uyy65By98jSwv6k3P2x3u7gypYL1Io/7hSyMiPIQCAEyOwMQVnl6ZPhRMBLJGMSYURg6qGMDikYNUh5XffIGt6oyo48/vgmopvhBURXl0fcGOZmwPYlKFaEEdW5kVkOeNokuGqIrz4z+BruarD4w9vGptZuElNhMKKKmsyBeh+WCi~1',
        }

        try:
            result = requests.get(PAGE, headers=headers)
            print(f"Accessing page {i} - Status code: {result.status_code}")

            # Add code to process the data here
            if result.status_code == 200:
                source = result.text
                soup = BeautifulSoup(source, 'html.parser')
                process_GPU_data(soup)
            else:
                print(f"Failed to fetch page {i}")

        except requests.exceptions.RequestException as er:
            print(f"An error of HTTP request: {er}")

if __name__ == "__main__":
    start_page = 1
    end_page = 1

    scraping_data(start_page, end_page)

