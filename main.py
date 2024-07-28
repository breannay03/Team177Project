import os
import pandas as pd
import ICViolationCheck as icv

def clean_menu_item_dupe_id_pair(menu_item):
    print('clean_menu_item_dupe_id_pair')
    cols_keep = menu_item.columns
    menu_item = menu_item.sort_values('updated_at')
    menu_item = menu_item[~menu_item[['dish_id', 'menu_page_id']].duplicated(keep='last')] # keep latest updated_at
    menu_item = menu_item.sort_values('id')
    return menu_item[cols_keep]

def clean_menu_item_created_updated_time(menu_item):
    print('clean_menu_item_created_updated_time')
    menu_item['created_at'] = pd.to_datetime(menu_item['created_at'])
    menu_item['updated_at'] = pd.to_datetime(menu_item['updated_at'])
    filt = ~(menu_item['created_at'] <= menu_item['updated_at'])
    menu_item.loc[filt, 'updated_at'] = menu_item.loc[filt, 'created_at']
    return menu_item

def clean_menu_page_menu_id(menu_page, menu):
    print('clean_menu_page_menu_id')
    return menu_page[menu_page['menu_id'].isin(menu['id'])]

def clean_menu_item_menu_page_id(menu_item, menu_page):
    print('clean_menu_item_menu_page_id')
    return menu_item[menu_item['menu_page_id'].isin(menu_page['id'])]

def clean_menu_item_dish_id(menu_item, dish):
    print('clean_menu_item_dish_id')
    menu_item['dish_id'] = menu_item['dish_id'].astype(int, errors='ignore')
    return menu_item[menu_item['dish_id'].isin(dish['id'])]

def clean_dish_date(menu, menu_page, menu_item, dish):
    print('clean_dish_date')
    # Merge dates from other menu tables with corresponding dishes
    dish_with_app = icv.merge_dish_with_menu_dates(menu, menu_page, menu_item, dish)
    dish_with_app['first_appeared'] = dish_with_app['first_appeared'].astype(int, errors='ignore')
    dish_with_app['last_appeared'] = dish_with_app['last_appeared'].astype(int, errors='ignore')
    dish_with_app['earliest_date'] = dish_with_app['earliest_date'].dt.year.astype(int, errors='ignore')
    dish_with_app['latest_date'] = dish_with_app['latest_date'].dt.year.astype(int, errors='ignore')
    
    # Populate first_appeared and last_appeared with values from earlier and latest menus
    dish_with_app['first_appeared'] = dish_with_app['earliest_date']
    dish_with_app['last_appeared'] = dish_with_app['latest_date']
    
    # If other column has valid year, copy for both columns
    dish_with_app['first_appeared'].fillna(dish_with_app['last_appeared'], inplace=True)
    dish_with_app['last_appeared'].fillna(dish_with_app['first_appeared'], inplace=True)

    # Drop remaining null values
    dish_with_app = dish_with_app[(dish_with_app['first_appeared'].notna() & dish_with_app['last_appeared'].notna())]
    
    cols_keep = dish.columns

    return dish_with_app[cols_keep].drop_duplicates()

def clean_price_dish(menu_item, dish):
    print('clean_price_dish')
    dish_mi = dish.merge(menu_item, left_on='id', right_on='dish_id', suffixes=('', '_mi'), how='left')
    dish_mi = dish_mi[~(dish_mi['price'].isna() & dish_mi['lowest_price'].isna() & dish_mi['highest_price'].isna())]
    
    # Prices from menu_item['price']
    dish_mi['price_low'] = dish_mi.groupby('id')['price'].transform('min')
    dish_mi['price_high'] = dish_mi.groupby('id')['price'].transform('max')
    dish_mi['lowest_price'].fillna(dish_mi['price_low'], inplace=True)
    dish_mi['highest_price'].fillna(dish_mi['price_high'], inplace=True)
    cols_keep = dish.columns
    dish_mi = dish_mi[cols_keep].drop_duplicates()
    dish_mi['first_appeared'] = dish_mi['first_appeared'].astype(int)
    dish_mi['last_appeared'] = dish_mi['last_appeared'].astype(int)
    return dish_mi

def clean_price_menu_item(menu_item, dish):
    print('clean_price_menu_item')
    menu_item_dish = menu_item.merge(dish, left_on='dish_id', right_on='id', suffixes=('', '_dish'), how='left')
    menu_item_dish = menu_item_dish[~(menu_item_dish['price'].isna() & menu_item_dish['lowest_price'].isna() & menu_item_dish['highest_price'].isna())]
    menu_item_dish['lowest_price'].fillna(menu_item_dish[['price', 'highest_price']].min(axis=1), inplace=True)
    menu_item_dish['highest_price'].fillna(menu_item_dish[['lowest_price', 'price']].max(axis=1), inplace=True)
    menu_item_dish['price'].fillna(menu_item_dish[['lowest_price', 'highest_price']].mean(axis=1), inplace=True)
    
    # Capping out-of-bound prices to boundaries of interval [lowest_price, highest_price]
    filt_low = menu_item_dish['price'] < menu_item_dish['lowest_price']
    filt_high = menu_item_dish['price'] > menu_item_dish['highest_price']
    menu_item_dish.loc[filt_low, 'price'] = menu_item_dish.loc[filt_low, 'lowest_price']
    menu_item_dish.loc[filt_high, 'price'] = menu_item_dish.loc[filt_high, 'highest_price']
    
    cols_keep = [col for col in menu_item.columns if col != 'high_price']
    
    return menu_item_dish[cols_keep].drop_duplicates()



############
### MAIN ###
############
    
if __name__=='__main__':
    CURDIR = os.getcwd()
    OR_DIR = os.path.join(CURDIR, 'OR_cleaned')
    CLEAN_DIR = os.path.join(CURDIR, 'clean')
    
    # @BEGIN main
    # @PARAM OR_DIR
    # @PARAM CLEAN_DIR
    # @OUT menu      @AS menu_full_cleaned      @URI file:{CLEAN_DIR}/Menu.csv
    # @OUT menu_page @AS menu_page_full_cleaned @URI file:{CLEAN_DIR}/MenuPage.csv
    # @OUT menu_item @AS menu_item_full_cleaned @URI file:{CLEAN_DIR}/MenuItem.csv
    # @OUT dish      @AS dish_full_cleaned      @URI file:{CLEAN_DIR}/Dish.csv
    
    
    
    # @BEGIN load_data
    # @PARAM OR_DIR
    # @OUT menu      @AS menu_full_cleaned    @URI file:{CLEAN_DIR}/Menu.csv
    # @OUT menu_page @AS menu_page_OR_cleaned @URI file:{OR_DIR}/MenuPage.csv
    # @OUT menu_item @AS menu_item_OR_cleaned @URI file:{OR_DIR}/MenuItem.csv
    # @OUT dish      @AS dish_OR_cleaned      @URI file:{OR_DIR}/Dish.csv
    menu, menu_page, menu_item, dish = icv.load_csv(OR_DIR)
    # @END load_data
    
    
    
    # @BEGIN clean_menu_item_created_updated_time
    # @IN menu_item @AS menu_item_OR_cleaned
    # @OUT menu_item @AS menu_item_correct_update_time
    menu_item = clean_menu_item_created_updated_time(menu_item)
    # @END clean_menu_item_created_updated_time
    
    # @BEGIN clean_menu_page_menu_id
    # @IN menu_page @AS menu_page_OR_cleaned @URI file:{OR_DIR}/MenuPage.csv
    # @IN menu      @AS menu_full_cleaned    @URI file:{CLEAN_DIR}/Menu.csv
    # @OUT menu_page @AS menu_page_full_cleaned @URI file:{CLEAN_DIR}/MenuPage.csv
    menu_page = clean_menu_page_menu_id(menu_page, menu)
    # @END clean_menu_page_menu_id
    
    # @BEGIN clean_menu_item_menu_page_id
    # @IN menu_item @AS menu_item_correct_update_time
    # @IN menu_page @AS menu_page_full_cleaned
    # @OUT menu_item @AS menu_item_with_clean_menu_page_id
    menu_item = clean_menu_item_menu_page_id(menu_item, menu_page)
    # @END clean_menu_item_menu_page_id
    
    # @BEGIN clean_dish_date
    # @IN menu      @AS menu_full_cleaned @URI file:{CLEAN_DIR}/Menu.csv
    # @IN menu_page @AS menu_page_full_cleaned
    # @IN menu_item @AS menu_item_with_clean_menu_page_id
    # @IN dish      @AS dish_OR_cleaned
    # @OUT dish @AS dish_with_clean_date
    dish = clean_dish_date(menu, menu_page, menu_item, dish)
    # @END clean_dish_date
    
    # @BEGIN clean_price_dish
    # @IN menu_item @AS menu_item_with_clean_menu_page_id
    # @IN dish      @AS dish_with_clean_date
    # @OUT dish @AS dish_full_cleaned @URI file:{CLEAN_DIR}/Dish.csv
    dish = clean_price_dish(menu_item, dish)
    # @END clean_price_dish
    
    # @BEGIN clean_price_menu_item
    # @IN menu_item @AS menu_item_with_clean_menu_page_id
    # @IN dish @AS dish_full_cleaned @URI file:{CLEAN_DIR}/Dish.csv
    # @OUT menu_item @AS menu_item_with_clean_price
    menu_item = clean_price_menu_item(menu_item, dish)
    # @END clean_price_menu_item
    
    # @BEGIN clean_menu_item_dupe_id_pair
    # @IN menu_item @AS menu_item_with_clean_price
    # @OUT menu_item @AS menu_item_no_dupe_id_pairs
    menu_item = clean_menu_item_dupe_id_pair(menu_item)
    # @END clean_menu_item_dupe_id_pair
    
    # @BEGIN clean_menu_item_dish_id
    # @IN menu_item @AS menu_item_no_dupe_id_pairs
    # @IN dish @AS dish_full_cleaned @URI file:{CLEAN_DIR}/Dish.csv
    # @OUT menu_item @AS menu_item_full_cleaned @URI file:{CLEAN_DIR}/MenuItem.csv
    menu_item = clean_menu_item_dish_id(menu_item, dish)
    # @END clean_menu_item_dish_id
    

    print('write to file')
    menu.to_csv(os.path.join(CLEAN_DIR, 'Menu.csv'))
    menu_page.to_csv(os.path.join(CLEAN_DIR, 'MenuPage.csv'))
    menu_item.to_csv(os.path.join(CLEAN_DIR, 'MenuItem.csv'))
    dish.to_csv(os.path.join(CLEAN_DIR, 'Dish.csv'))
    
    # @END main
    
    