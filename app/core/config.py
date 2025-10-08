from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    # Database
    db_server: str = Field("localhost", env="DB_SERVER")
    db_name: str = Field("your_database", env="DB_NAME")
    db_user: str = Field("your_username", env="DB_USER")
    db_password: str = Field("your_password", env="DB_PASSWORD")
    db_driver: str = Field("ODBC Driver 18 for SQL Server", env="DB_DRIVER")
    
    # Shopify
    shopify_shop_url: str = Field("https://your-shop.myshopify.com", env="SHOPIFY_SHOP_URL")
    shopify_access_token: str = Field("your_access_token", env="SHOPIFY_ACCESS_TOKEN")
    shopify_api_version: str = Field("2024-10", env="SHOPIFY_API_VERSION")
    
    # Wortmann FTP / Files
    wortmann_ftp_host: str = Field("", env="WORTMANN_FTP_HOST")
    wortmann_ftp_port: int = Field(21, env="WORTMANN_FTP_PORT")
    wortmann_ftp_user: str = Field("", env="WORTMANN_FTP_USER")
    wortmann_ftp_password: str = Field("", env="WORTMANN_FTP_PASSWORD")
    wortmann_path_productcatalog: str = Field("/Preisliste/productcatalog.csv", env="WORTMANN_PATH_PRODUCTCATALOG")
    wortmann_path_content: str = Field("/Preisliste/content.csv", env="WORTMANN_PATH_CONTENT")
    wortmann_path_images_zip: str = Field("/Produktbilder/productimages.zip", env="WORTMANN_PATH_IMAGES_ZIP")
    
    # API
    api_host: str = Field("0.0.0.0", env="API_HOST")
    api_port: int = Field(8000, env="API_PORT")
    debug: bool = Field(False, env="DEBUG")
    
    # App
    app_name: str = Field("N8N Workflow API", env="APP_NAME")
    version: str = Field("1.0.0", env="VERSION")
    
    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()
