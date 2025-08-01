from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any


class ProductBase(BaseModel):
    ProductId: str
    Title: Optional[str] = None
    LongDescription: Optional[str] = None
    DescriptionShort: Optional[str] = None
    Manufacturer: Optional[str] = None
    Category: Optional[str] = None
    CategoryPath: Optional[str] = None
    Price_B2C_inclVAT: Optional[float] = None
    Price_B2B_Regular: Optional[float] = None
    Stock: Optional[int] = None
    GrossWeight: Optional[float] = None
    NetWeight: Optional[float] = None
    Warranty: Optional[str] = None
    Garantiegruppe: Optional[int] = None


class ImageBase(BaseModel):
    supplier_aid: str
    base64: Optional[str] = None
    hex: Optional[str] = None


class WarrantyBase(BaseModel):
    id: int
    name: str
    monate: Optional[int] = None
    prozentsatz: Optional[float] = None
    minimum: Optional[float] = None
    garantiegruppe: Optional[int] = None


class ProcessedProduct(BaseModel):
    product_id: str
    title: Optional[str] = None
    handle: str
    base_price: float
    quantity: int
    weight: float
    has_warranty_group: bool
    images: List[str] = []
    warranties: List[WarrantyBase] = []


class WorkflowRequest(BaseModel):
    product_limit: int = Field(default=None)
    image_limit: int = Field(default=None)
    batch_size: int = Field(default=None)
    dry_run: bool = Field(default=False)


class WorkflowResponse(BaseModel):
    status: str
    message: str
    total_products: int
    successful_uploads: int = 0
    failed_uploads: int = 0
    execution_time: float
    results: List[Dict[str, Any]] = []
