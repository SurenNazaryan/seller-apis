import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получает список товаров из магазина OZON.

    Эта функция отправляет запрос к API OZON для получения списка товаров,
    начиная с указанного идентификатора последнего товара.

    Args:
        last_id (str): Идентификатор последнего товара, полученного в предыдущем запросе.
        client_id (str): Идентификатор клиента для API Ozon.
        seller_token (str): Токен продавца для API Ozon.

    Returns:
        list: Список товаров в магазине Ozon.

    Example:
        >>> get_product_list("12345", "my_client_id", "my_seller_token")
        [{'offer_id': '123', 'name': 'Product A'}, {'offer_id': '124', 'name': 'Product B'}]

    Raises:
        requests.exceptions.HTTPError: Если запрос к API завершился неудачно.
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получает все артикулы товаров из магазина Ozon.

    Эта функция собирает все артикулы товаров, используя постраничный запрос
    к API OZON.

    Args:
        client_id (str): Идентификатор клиента для API Ozon.
        seller_token (str): Токен продавца для API Ozon.

    Returns:
        list: Список артикулов товаров в магазине Ozon.

    Example:
        >>> get_offer_ids("my_client_id", "my_seller_token")
        ['offer_id_1', 'offer_id_2', 'offer_id_3']

    Raises:
        requests.exceptions.HTTPError: Если запрос к API завершился неудачно.
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновляет цены товаров.

    Эта функция отправляет обновленные цены товаров в API OZON.

    Args:
        prices (list): Список цен для обновления.
        client_id (str): Идентификатор клиента для API Ozon.
        seller_token (str): Токен продавца для API Ozon.

    Returns:
        dict: Ответ API OZON.

    Example:
        >>> update_price([{'offer_id': '123', 'price': '1000'}], "my_client_id", "my_seller_token")
        {'success': True}

    Raises:
        requests.exceptions.HTTPError: Если запрос к API завершился неудачно.
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновляет остатки товаров.

    Эта функция отправляет обновленные остатки товаров в API OZON.

    Args:
        stocks (list): Список остатков для обновления.
        client_id (str): Идентификатор клиента для API Ozon.
        seller_token (str): Токен продавца для API Ozon.

    Returns:
        dict: Ответ API OZON.

    Example:
        >>> update_stocks([{'offer_id': '123', 'stock': 10}], "my_client_id", "my_seller_token")
        {'success': True}

    Raises:
        requests.exceptions.HTTPError: Если запрос к API завершился неудачно.
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачивает файл остатков с сайта Casio.

    Эта функция загружает zip-архив с остатками товаров и извлекает данные
    в формате Excel.

    Returns:
        list: Список остатков часов в формате словаря.

    Example:
        >>> download_stock()
        [{'Код': '123', 'Количество': '5', 'Цена': '5\'990.00 руб.'}, ...]

    Raises:
        requests.exceptions.HTTPError: Если запрос к сайту завершился неудачно.
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Создает список остатков на основе данных о часах.

    Эта функция сопоставляет остатки часов с загруженными артикулов
    и формирует список остатков для обновления.

    Args:
        watch_remnants (list): Список остатков часов.
        offer_ids (list): Список артикулов товаров.

    Returns:
        list: Список остатков для обновления.

    Example:
        >>> create_stocks([{'Код': '123', 'Количество': '5'}], ['123', '124'])
        [{'offer_id': '123', 'stock': 5}, {'offer_id': '124', 'stock': 0}]

    Raises:
        ValueError: Если данные о часах некорректные.
    """
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создает список цен на основе данных о часах.

    Эта функция сопоставляет цены часов с загруженными артикулов
    и формирует список цен для обновления.

    Args:
        watch_remnants (list): Список остатков часов.
        offer_ids (list): Список артикулов товаров.

    Returns:
        list: Список цен для обновления.

    Example:
        >>> create_prices([{'Код': '123', 'Цена': '5\'990.00 руб.'}], ['123', '124'])
        [{'auto_action_enabled': 'UNKNOWN', 'currency_code': 'RUB',
        'offer_id': '123', 'old_price': '0', 'price': '5990'}]
    """    
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразует строковое представление цены в числовой формат.

    Args:
        price (str): Строка, представляющая цену.

    Returns:
        str: Числовое представление цены.

    Example:
        >>> price_conversion("5'990.00 руб.")
        '5990'
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделяет список на части по n элементов.

    Args:
        lst (list): Список, который нужно разделить.
        n (int): Количество элементов в каждой части.

    Yields:
        list: Подсписки длиной n.
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Загружает цены на товары в API.

    Эта функция получает артикулы, создает список цен и загружает
    их в API Ozon.

    Args:
        watch_remnants (list): Список остатков часов.
        client_id (str): Идентификатор клиента для API Ozon.
        seller_token (str): Токен продавца для API Ozon.

    Returns:
        list: Список загруженных цен.
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Загружает остатки товаров в API.

    Эта функция получает артикулы, создает список остатков и загружает
    их в API Ozon.

    Args:
        watch_remnants (list): Список остатков часов.
        client_id (str): Идентификатор клиента для API Ozon.
        seller_token (str): Токен продавца для API Ozon.

    Returns:
        tuple: Список ненулевых остатков и полный список остатков.
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    """Основная функция для обновления остатков и цен товаров."""
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
