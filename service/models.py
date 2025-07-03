""" Models for the Monarch service """
from enum import Enum
from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel


class MigrationStatus(str, Enum):
    """Enum representing the status of an issue migration"""
    PENDING = "pending"
    IN_FLIGHT = "in_flight"
    COMPLETED = "completed"
    FAILED = "failed"

class MigrationState(BaseModel):
    """Tracks the state of an issue migration"""
    sequence_id: int
    source_repo: str
    target_repo: str
    issue_id: str  # Unique identifier for the issue
    issue_data: dict  # The issue data to be migrated
    status: MigrationStatus = MigrationStatus.PENDING
    attempts: int = 0
    last_attempt: Optional[datetime] = None
    error_message: Optional[str] = None
    completed_at: Optional[datetime] = None

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
