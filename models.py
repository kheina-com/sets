from typing import Optional

from fuzzly.models.post import PostId
from fuzzly.models.set import SetId
from fuzzly.models.user import UserPrivacy
from pydantic import BaseModel, conint, conlist


class CreateSetRequest(BaseModel) :
	title: str
	description: Optional[str]
	privacy: UserPrivacy


class UpdateSetRequest(BaseModel) :
	mask: conlist(str, min_items=1)
	owner: Optional[str]
	title: Optional[str]
	description: Optional[str]
	privacy: Optional[UserPrivacy]


class AddPostToSetRequest(BaseModel) :
	post_id: PostId
	set_id: SetId
	index: conint(ge=0)
