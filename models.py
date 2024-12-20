from pydantic import BaseModel
from typing import List, Optional

class IssueTemplate(BaseModel):
    user_name: str
    user_url: str
    user_avatar: str
    date: str
    url: str
    body: str

class Issue(BaseModel):
    title: str
    body: str

class MigrationData(BaseModel):
    source_repo: str
    all_target_repositories: List[str]
    notification_channel: str