from pydantic import BaseModel
from typing import List, Optional

class JobListing(BaseModel):
    title: str
    url: str
    description: Optional[str] = ""

class CompanyResult(BaseModel):
    company_name: str
    career_page_url: Optional[str] = None
    job_listings: List[JobListing]

class WebLookupOutput(BaseModel):
    results: List[CompanyResult]
