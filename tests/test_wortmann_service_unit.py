from app.services.wortmann_service import WortmannService


def test_normalize_products_basic(monkeypatch):
    svc = WortmannService(db_service=None)

    # Disable ftp and db for this unit-level test by monkeypatching internal helpers
    monkeypatch.setattr(svc, "_ftp_download", lambda *_: b"")
    rows = [{
        'ProductId': '123',
        'Description_1031_German': 'T',
        'LongDescription_1031_German': 'L',
        'Manufacturer': 'W',
        'CategoryName_1031_German': 'PC',
        'CategoryPath_1031_German': 'A|B',
        'WarrantyDescription_1031_German': 'Std',
        'Price_B2B_Regular': '10,0',
        'Price_B2B_Discounted': '9,0',
        'Price_B2C_inclVAT': '12,0',
        'Price_B2C_VATRate': '19',
        'Stock': '5',
        'GrossWeight': '1,2',
        'NetWeight': '1,0',
        'NonReturnable': '0',
        'EOL': '0',
        'Promotion': '0',
        'AccessoryProducts': '',
        'Category': 'pc',
        'StockNextDelivery': '2025-09-29',
    }]

    normalized = svc._normalize_products(rows)
    assert normalized and normalized[0]['ProductId'] == '123'
    assert normalized[0]['Price_B2B_Regular'] == 10.0
    assert normalized[0]['StockNextDelivery'] == '29.09.2025'


