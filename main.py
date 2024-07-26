import os
import pandas as pd
import ICViolationCheck as icv

CURDIR = os.getcwd()
RAWDIR = os.path.join(CURDIR, 'raw')
ORDIR = os.path.join(CURDIR, 'OR_cleaned')

def clean_menu_page_menu_id(menu_page, menu):
    violated_ids = icv.check_menu_id(menu_page, menu)
    return menu_page[~menu_page['id'].isin(violated_ids)]

def clean_menu_item_menu_page_id(menu_item, menu_page):
    violated_ids = icv.check_menu_page_id(menu_item, menu_page)
    return menu_item[~menu_item['id'].isin(violated_ids)]

def clean_menu_item_dish_id(menu_item, dish):
    violated_ids = icv.check_dish_id(menu_item, dish)
    return menu_item[~menu_item['id'].isin(violated_ids)]

def clean_dish_date(menu, menu_page, menu_item, dish):
    # Merge dates from other menu tables with corresponding dishes
    dish_with_app = icv.merge_dish_with_menu_dates(menu, menu_page, menu_item, dish)
    dish_with_app['first_appeared'] = dish_with_app['first_appeared'].astype(int, errors='ignore')
    dish_with_app['last_appeared'] = dish_with_app['last_appeared'].astype(int, errors='ignore')
    dish_with_app['earliest_date'] = dish_with_app['earliest_date'].dt.year.astype(int, errors='ignore')
    dish_with_app['latest_date'] = dish_with_app['latest_date'].dt.year.astype(int, errors='ignore')
    
    # Populate first_appeared and last_appeared with values from earlier and latest menus
    dish_with_app['first_appeared'].fillna(dish_with_app['earliest_date'], inplace=True)
    dish_with_app['last_appeared'].fillna(dish_with_app['latest_date'], inplace=True)
    
    # If other column has valid year, copy for both columns
    dish_with_app['first_appeared'].fillna(dish_with_app['last_appeared'], inplace=True)
    dish_with_app['last_appeared'].fillna(dish_with_app['first_appeared'], inplace=True)

    # Drop remaining null values
    dish_with_app = dish_with_app[dish_with_app['first_appeared'].notna()]
    
    cols_keep = dish.columns

    return dish_with_app[cols_keep]

def clean_menu_item_price(menu_item, dish):
    menu_item_dish = menu_item.merge(dish, left_on='dish_id', right_on='id', suffixes=('', '_dish'))

if __name__=='__main__':
    # _ = icv.run_integrity_checks(RAWDIR)
    
    menu = pd.read_csv(os.path.join(ORDIR, 'Menu.csv'))
    menu_item = pd.read_csv(os.path.join(ORDIR, 'MenuItem.csv'))
    menu_page = pd.read_csv(os.path.join(ORDIR, 'MenuPage.csv'))
    dish = pd.read_csv(os.path.join(ORDIR, 'Dish.csv'))
    
    menu_page = clean_menu_page_menu_id(menu_page, menu)
    menu_item = clean_menu_item_menu_page_id(menu_item, menu_page)
    menu_item = clean_menu_item_dish_id(menu_item, dish)
    dish = clean_dish_date(menu, menu_page, menu_item, dish)
    
    