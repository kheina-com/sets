from typing import List

from fuzzly.models.internal import InternalSet
from fuzzly.models.post import PostId
from fuzzly.models.set import PostSet, Set, SetId
from kh_common.auth import Scope
from kh_common.server import Request, ServerApp

from models import AddPostToSetRequest, CreateSetRequest, UpdateSetRequest
from sets import Sets


app = ServerApp(
	auth_required = False,
	allowed_hosts = [
		'localhost',
		'127.0.0.1',
		'*.fuzz.ly',
		'fuzz.ly',
	],
	allowed_origins = [
		'localhost',
		'127.0.0.1',
		'dev.fuzz.ly',
		'fuzz.ly',
	],
)
sets = Sets()


@app.on_event('shutdown')
async def shutdown() :
	sets.close()


################################################## INTERNAL ##################################################

@app.get('/i1/set/{set_id}')
async def i1Read(req: Request, set_id: SetId) -> InternalSet :
	await req.user.verify_scope(Scope.internal)
	return await sets._get_set(SetId(set_id))


##################################################  PUBLIC  ##################################################

@app.put('/v1/set')
async def v1Create(req: Request, body: CreateSetRequest) -> Set :
	await req.user.authenticated()
	return await sets.create_set(req.user, body.title, body.privacy, body.description)


@app.get('/v1/set/{set_id}')
async def v1Read(req: Request, set_id: SetId) -> Set :
	return await sets.get_set(req.user, SetId(set_id))


@app.patch('/v1/set/{set_id}', status_code=204)
async def v1Update(req: Request, set_id: SetId, body: UpdateSetRequest) -> None :
	await req.user.authenticated()
	return await sets.update_set(req.user, SetId(set_id), body)


@app.delete('/v1/set/{set_id}', status_code=204)
async def v1Update(req: Request, set_id: SetId) -> None :
	await req.user.authenticated()
	return await sets.delete_set(req.user, SetId(set_id))


@app.get('/v1/post/{post_id}')
async def v1PostSets(req: Request, post_id: PostId) -> List[PostSet] :
	return await sets.get_post_sets(req.user, PostId(post_id))


@app.put('/v1/post', status_code=204)
async def v1AddPost(req: Request, body: AddPostToSetRequest) -> None :
	await req.user.authenticated()
	return await sets.add_post_to_set(req.user, body.post_id, body.set_id, body.index)


@app.delete('/v1/post/{post_id}/{set_id}', status_code=204)
async def v1AddPost(req: Request, post_id: PostId, set_id: SetId) -> None :
	await req.user.authenticated()
	return await sets.remove_post_from_set(req.user, PostId(post_id), SetId(set_id))


@app.get('/v1/user/{handle}')
async def v1UserSets(req: Request, handle: str) -> List[Set] :
	return await sets.get_user_sets(req.user, handle)


if __name__ == '__main__' :
	from uvicorn.main import run
	run(app, host='0.0.0.0', port=5008)
