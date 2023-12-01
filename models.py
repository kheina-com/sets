from typing import Optional

from fuzzly.models.post import PostId, PostIdValidator
from fuzzly.models.set import SetId, SetIdValidator
from fuzzly.models.user import UserPrivacy
from pydantic import BaseModel, conint, conlist, constr


class CreateSetRequest(BaseModel) :
	title: constr(max_length=50)
	description: Optional[str]
	privacy: UserPrivacy


class UpdateSetRequest(BaseModel) :
	mask: conlist(str, min_items=1)
	owner: Optional[str]
	title: Optional[constr(max_length=50)]
	description: Optional[str]
	privacy: Optional[UserPrivacy]


class AddPostToSetRequest(BaseModel) :
	_post_id_validator = PostIdValidator
	_set_id_validator = SetIdValidator

	post_id: PostId
	set_id: SetId
	index: conint(ge=0)
