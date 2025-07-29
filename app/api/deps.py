from app.services.database_service import database_service
from app.services.product_service import product_service
from app.services.shopify_service import shopify_service


def get_database_service():
    """Dependency for database service"""
    return database_service


def get_product_service():
    """Dependency for product service"""
    return product_service


def get_shopify_service():
    """Dependency for Shopify service"""
    return shopify_service
