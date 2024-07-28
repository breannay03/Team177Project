import os
import pandas as pd


def load_csv(filedir):
    menu      = pd.read_csv(os.path.join(filedir, 'Menu.csv'))
    menu_page = pd.read_csv(os.path.join(filedir, 'MenuPage.csv'))
    menu_item = pd.read_csv(os.path.join(filedir, 'MenuItem.csv'))
    dish      = pd.read_csv(os.path.join(filedir, 'Dish.csv'))
    return menu, menu_page, menu_item, dish



# Finds ids in MenuItem where the dish_id and menu_page_id are the same as another id
def check_duplicate_page_dish(menu_item):
    # Find duplicate (dish_id, menu_page_id) pairs
    invalid_filter = menu_item.duplicated(subset=['dish_id', 'menu_page_id'], keep=False)
    duplicate_ids = menu_item[invalid_filter]['id'].tolist()

    # Returns list of tuples of ids that have same (dish_id, menu_page_id) pair
    return duplicate_ids



# Find ids in MenuItem where created_at is later than updated_at
def check_menu_item_created_updated_time(menu_item):
    invalid_filter = ~(pd.to_datetime(menu_item['created_at']) <= pd.to_datetime(menu_item['updated_at']))
    return menu_item[invalid_filter]['id'].tolist()



# Checks that every menu_id in MenuPage is a valid id in Menu
def check_menu_id(menu_page, menu):
    # invalid_filter is a series of boolean values where True means MenuPage's menu_id is not an id in Menu
    invalid_filter = ~menu_page['menu_id'].isin(menu['id'])

    # Keeps only invalid ids and returns as a list
    return menu_page[invalid_filter]['id'].tolist()



# Checks that every menu_page_id in MenuItem is a valid id in MenuPage
def check_menu_page_id(menu_item, menu_page):
    # invalid_filter is a series of boolean values where True means MenuItem's menu_page_id is not an id in MenuPage
    invalid_filter = ~menu_item['menu_page_id'].isin(menu_page['id'])

    # Keeps only invalid ids and returns as a list
    return menu_item[invalid_filter]['id'].tolist()



# Checks that every dish_id in MenuItem is a valid id in Dish
def check_dish_id(menu_item, dish):
    # invalid_filter is a series of boolean values where True means MenuItem's dish_id is not an id in Dish
    invalid_filter = ~menu_item['dish_id'].isin(dish['id'])

    # Keeps only invalid ids and returns as a list
    return menu_item[invalid_filter]['id'].tolist()



# Finds and merges earliest and latest dates of menu containing the corresponding dishes into the dish dataframe
def merge_dish_with_menu_dates(menu, menu_page, menu_item, dish):
    # First join MenuItem with MenuPage on menu_page_id = id, then join this with Menu on menu_id = id
    menu_item_page = menu_item.merge(menu_page, left_on='menu_page_id', right_on='id', suffixes=('_menu_item', '_menu_page'))
    menu_merged = menu_item_page.merge(menu, left_on='menu_id', right_on='id', suffixes=('', '_menu'))

    # Filter relevant columns and convert date column to correct format
    merged = menu_merged[['dish_id', 'date', 'menu_id']]
    merged['date'] = pd.to_datetime(merged['date'], errors='coerce')

    # Create two new dataframes consisting of two columns, dish_id and date, where date is the earliest/latest the dish appears in Menu
    earliest_dates = merged.groupby('dish_id')['date'].min().reset_index()
    latest_dates = merged.groupby('dish_id')['date'].max().reset_index()
    earliest_dates = earliest_dates.rename(columns={'date': 'earliest_date'})
    latest_dates = latest_dates.rename(columns={'date': 'latest_date'})

    # Join earliest_dates and latest_dates with Dish and convert date columns to correct format
    dish_with_app = dish.merge(earliest_dates, left_on = 'id', right_on='dish_id', how='left').merge(latest_dates, left_on = 'id', right_on='dish_id', how='left')
    return dish_with_app



# Checks that first_appeared and last_appeared are between (1800, 2024), first_appeared <= last_appeared, 
# and first_appeared and last_appeared correspond to the dates the dish first and last appeared in Menu
def check_date_validity(menu, menu_page, menu_item, dish):
    dish_with_app = merge_dish_with_menu_dates(menu, menu_page, menu_item, dish)
    dish_with_app = dish_with_app[['id', 'first_appeared', 'last_appeared', 'earliest_date', 'latest_date']]

    # invalid_filter is a series of boolean values where True means first_appeared and last_appeared are not in between 1800 to 2024 or first_appeared >= last_appeared
    dish_with_app['first_appeared'] = dish_with_app['first_appeared'].astype(int, errors='ignore')
    dish_with_app['last_appeared'] = dish_with_app['last_appeared'].astype(int, errors='ignore')
    invalid_filter = ~((dish_with_app['first_appeared'].between(1800, 2024, inclusive='both')) 
                       & (dish_with_app['last_appeared'].between(1800, 2024, inclusive='both')) 
                       & (dish_with_app['first_appeared'] <= dish_with_app['last_appeared']))
    invalid_dish_ids = dish_with_app[invalid_filter]['id'].tolist()

    # inconsistent_filter is a series of boolean values where True means the dish's first_appeared or last_appeared is not the same as the earliest or latest year it appears in Menu
    dish_with_app['earliest_date'] = dish_with_app['earliest_date'].dt.year.astype(int, errors='ignore')
    dish_with_app['latest_date'] = dish_with_app['latest_date'].dt.year.astype(int, errors='ignore')
    inconsistent_filter = ~((dish_with_app['first_appeared']==dish_with_app['earliest_date']) & 
                            (dish_with_app['last_appeared']==dish_with_app['latest_date']))
    inconsistent_dates = dish_with_app[inconsistent_filter]['id'].tolist()

    # Return all the invalid ids
    return list(set(invalid_dish_ids + inconsistent_dates))



# Checks that MenuItem.price is between lowest_price and highest_price in Dish
def check_price_validity(menu_item, dish):
    # Join MenuItem and Dish on dish_id = id
    menu_item_dish = menu_item.merge(dish, left_on='dish_id', right_on='id', suffixes=('', '_dish'))
    
    # invalid_filter is a series of boolean values where True means price is outside of (lowest_price, highest_price)
    menu_item_dish['price'] = menu_item_dish['price'].astype(float)
    menu_item_dish['lowest_price'] = menu_item_dish['lowest_price'].astype(float)
    menu_item_dish['highest_price'] = menu_item_dish['highest_price'].astype(float)
    invalid_filter = ~(menu_item_dish['price'].between(menu_item_dish['lowest_price'], menu_item_dish['highest_price'], inclusive='both'))
    
    return menu_item_dish[invalid_filter]['id'].tolist()





def run_integrity_checks(filedir):
    # Load CSV files
    menu, menu_page, menu_item, dish = load_csv(filedir)

    dup_page_dish_ids = check_duplicate_page_dish(menu_item)
    invalid_update_time_ids = check_menu_item_created_updated_time(menu_item)
    invalid_menu_ids = check_menu_id(menu_page, menu)
    invalid_menu_page_ids = check_menu_page_id(menu_item, menu_page)
    invalid_dish_ids = check_dish_id(menu_item, dish)
    invalid_dish_appearance_ids = check_date_validity(menu, menu_page, menu_item, dish)
    invalid_price_ids = check_price_validity(menu_item, dish)

    print(f"{len(dup_page_dish_ids)} violations for duplicate (dish_id, menu_page_id) pairs in MenuItem")
    print(f"{len(invalid_update_time_ids)} violations for invalid update and create time in MenuItem")
    print(f"{len(invalid_menu_ids)} violations for invalid menu_id in MenuPage")
    print(f"{len(invalid_menu_page_ids)} violations for invalid menu_page_id in MenuItem")
    print(f"{len(invalid_dish_ids)} violations for invalid dish_id in MenuItem")
    print(f"{len(invalid_dish_appearance_ids)} violations for invalid dates in first and last appeared in Dish")
    print(f"{len(invalid_price_ids)} violations for invalid price in MenuItem")


if __name__ == "__main__":
    CURDIR = os.getcwd()
    RAWDIR = os.path.join(CURDIR, 'raw')
    ORDIR = os.path.join(CURDIR, 'OR_cleaned')
    CLEANDIR = os.path.join(CURDIR, 'clean')
    
    run_integrity_checks(RAWDIR)
    print('-'*50)
    run_integrity_checks(CLEANDIR)
