from fastapi import APIRouter, Depends
from app.services.database_service import DatabaseService
from app.services.shopify_service import ShopifyService
from app.api.deps import get_database_service, get_shopify_service

router = APIRouter()


@router.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "fastapi-n8n-workflow",
        "version": "1.0.0"
    }


@router.get("/test-connections")
async def test_connections(
    db_service: DatabaseService = Depends(get_database_service),
    shopify_service: ShopifyService = Depends(get_shopify_service)
):
    """Test database and Shopify connections"""
    results = {}
    
    # Test database connection
    try:
        results['database'] = 'connected' if db_service.test_connection() else 'failed'
    except Exception as e:
        results['database'] = f'error: {str(e)}'
    
    # Test Shopify connection
    try:
        is_connected = await shopify_service.test_connection()
        results['shopify'] = 'connected' if is_connected else 'failed'
    except Exception as e:
        results['shopify'] = f'error: {str(e)}'
    
    return results
