""" Models for the Monarch service """
from typing import List
from pydantic import BaseModel

class IssueTemplate(BaseModel):
    """ Data required to create a new issue from a template """
    user_name: str
    user_url: str
    user_avatar: str
    date: str
    url: str
    body: str

class Issue(BaseModel):
    """ Data required to create a new issue """
    title: str
    body: str

class MigrationData(BaseModel):
    """ JSON data required to migrate issues from one repository to another """
    source_repo: str
    all_target_repositories: List[str]
    notification_channel: str