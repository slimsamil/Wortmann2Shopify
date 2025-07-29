from pydantic import BaseModel
from typing import List, Optional, Dict, Any


class ShopifyVariant(BaseModel):
    price: str
    sku: str
    inventory_quantity: int
    inventory_management: Optional[str] = None
    inventory_policy: Optional[str] = None
    weight: float
    weight_unit: str = "kg"
    option1: str


class ShopifyOption(BaseModel):
    name: str
    values: List[str]


class ShopifyMetafield(BaseModel):
    namespace: str
    key: str
    value: str
    type: str


class ShopifyImage(BaseModel):
    attachment: str


class ShopifyProduct(BaseModel):
    title: str
    handle: str
    body_html: str
    vendor: Optional[str] = None
    product_type: Optional[str] = None
    tags: Optional[List[str]] = None
    variants: List[ShopifyVariant]
    options: List[ShopifyOption]
    metafields: List[ShopifyMetafield]
    images: Optional[List[ShopifyImage]] = None


class ShopifyProductWrapper(BaseModel):
    product: ShopifyProduct

