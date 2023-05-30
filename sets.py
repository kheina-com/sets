from asyncio import Task, ensure_future, wait
from typing import List, Optional, Tuple, Union

from fuzzly.internal import InternalClient
from fuzzly.models.internal import InternalSet, SetKVS
from fuzzly.models.post import PostId
from fuzzly.models.set import PostSet, Set, SetId, PostNeighbors
from fuzzly.models.user import UserPrivacy
from kh_common.auth import KhUser, Scope
from kh_common.caching import AerospikeCache, ArgsCache
from kh_common.caching.key_value_store import KeyValueStore
from kh_common.config.credentials import fuzzly_client_token
from kh_common.datetime import datetime
from kh_common.exceptions.http_error import BadRequest, HttpErrorHandler, NotFound
from kh_common.sql import SqlInterface
from kh_common.hashing import Hashable

from models import UpdateSetRequest


"""
CREATE TABLE kheina.public.sets (
	set_id BIGINT NOT NULL PRIMARY KEY,
	owner BIGINT NOT NULL REFERENCES kheina.public.users (user_id),
	title TEXT NULL,
	description TEXT NULL,
	privacy smallint NOT NULL REFERENCES kheina.public.privacy (privacy_id),
	created TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX sets_owner_idx ON kheina.public.sets (owner);
CREATE TABLE kheina.public.set_post (
	set_id BIGINT NOT NULL REFERENCES kheina.public.sets (set_id),
	post_id BIGINT NOT NULL REFERENCES kheina.public.posts (post_id),
	index INT NOT NULL,
	PRIMARY KEY (set_id, post_id),
	UNIQUE (post_id, set_id),
	UNIQUE (set_id, index)
);
"""

set_not_found_errstr: str = 'no data was found for the provided set id: {set_id}.'

KVS: KeyValueStore = KeyValueStore('kheina', 'sets')

client: InternalClient = InternalClient(fuzzly_client_token)


class Sets(SqlInterface, Hashable) :

	async def _verify_authorized(user: KhUser, iset: InternalSet) -> bool :
		return user.user_id == iset.set_id or await user.verify_scope(Scope.mod, raise_error=False)


	@ArgsCache(float('inf'))
	async def _id_to_privacy(self: 'Sets', privacy_id: int) -> UserPrivacy :
		data: Tuple[str] = await self.query_async("""
			SELECT
				type
			FROM kheina.public.privacy
				where privacy.privacy_id = %s;
			""",
			(privacy_id,),
			fetch_one=True,
		)

		return UserPrivacy(data[0])


	@HttpErrorHandler('creating a set')
	async def create_set(self: 'Sets', user: KhUser, title: str, privacy: UserPrivacy, description: Optional[str]) :
		set_id: SetId

		while True :
			set_id = SetId.generate()
			data = await self.query_async("""
				SELECT count(1)
				FROM kheina.public.sets
				WHERE set_id = %s;
				""",
				(set_id.int(),),
				fetch_one=True
			)

			if not data[0] :
				break

		data: Tuple[datetime, datetime] = await self.query_async("""
			INSERT INTO kheina.public.sets
			(set_id, owner, title, description, privacy)
			VALUES
			(%s, %s, %s, %s, privacy_to_id(%s))
			RETURNING created, updated;
			""",
			(set_id.int(), user.user_id, title, description, privacy.name),
			fetch_one=True,
			commit=True,
		)

		iset: InternalSet = InternalSet(
			set_id=set_id.int(),
			owner=user.user_id,
			title=title,
			description=description,
			privacy=privacy,
			created=data[0],
			updated=data[1],
		)

		ensure_future(SetKVS.put_async(set_id, iset))


	@AerospikeCache('kheina', 'sets', '{set_id}', _kvs=SetKVS)
	async def _get_set(self: 'Sets', set_id: SetId) -> InternalSet :
		data: Tuple[int, Optional[str], Optional[str], int, datetime, datetime] = await self.query_async("""
			SELECT
				owner,
				title,
				description,
				privacy,
				created,
				updated
			FROM kheina.public.sets
			WHERE set_id = %s;
			""",
			(set_id.int(),),
			fetch_one=True,
		)

		if not data: 
			raise NotFound(set_not_found_errstr.format(set_id=set_id))

		return InternalSet(
			set_id=set_id,
			owner=data[0],
			title=data[1],
			description=data[2],
			privacy=await self._id_to_privacy(data[3]),
			created=data[4],
			updated=data[5],
		)


	@HttpErrorHandler('retrieving set')
	async def get_set(self: 'Sets', user: KhUser, set_id: SetId) -> Set :
		iset: InternalSet = self._get_set(set_id)

		if await iset.authorized(client, user) :
			return await iset.set(client, user)

		raise NotFound(set_not_found_errstr.format(set_id=set_id))


	@HttpErrorHandler('updating a set')
	async def update_set(self: 'Sets', user: KhUser, set_id: SetId, req: UpdateSetRequest) :
		iset: InternalSet = await self._get_set(set_id)

		if not Sets._verify_authorized(user, iset) :
			raise NotFound(set_not_found_errstr.format(set_id=set_id))

		params: List[Union[str, UserPrivacy, None]] = []
		bad_mask: List[str] = []
		query: List[str] = []

		for m in req.mask :
			query.append(m + ' = %s')

			if m == 'owner' :
				owner: int = await client.user_handle_to_id(req.owner)
				params.append(owner)
				iset.owner = owner

			elif m == 'title' :
				params.append(req.title)
				iset.title = req.title

			elif m == 'description' :
				params.append(req.description)
				iset.description = req.description

			elif m == 'privacy' :
				params.append(req.privacy.name)
				iset.privacy = req.privacy

			else :
				bad_mask.append(m)

		if bad_mask :
			if len(bad_mask) == 1 :
				raise BadRequest(f'[{bad_mask[0]}] is not a valid mask value')

			else :
				raise BadRequest(f'[{bad_mask.join(", ")}] are not valid mask values')

		params.append(set_id.int())

		data: Tuple[datetime] = await self.query_async(f"""
			UPDATE kheina.public.sets
				{query.join(', ')}
			WHERE set_id = %s
			RETURNING updated;
			""",
			tuple(params),
			fetch_one=True,
			commit=True,
		)

		iset.updated = data[0]
		ensure_future(SetKVS.put_async(set_id, iset))


	@HttpErrorHandler('deleting a set')
	async def delete_set(self: 'Sets', user: KhUser, set_id: SetId) -> None :
		iset: InternalSet = await self._get_set(set_id)

		if not Sets._verify_authorized(user, iset) :
			raise NotFound(set_not_found_errstr.format(set_id=set_id))

		await self.query_async("""
			DELETE FROM kheina.public.set_post
			WHERE set_id = %s;
			DELETE FROM kheina.public.sets
			WHERE set_id = %s;
			""",
			(set_id, set_id),
			commit=True,
		)

		await SetKVS.remove_async(set_id)


	async def get_post_sets(self: 'Sets', user: KhUser, post_id: PostId) -> List[PostSet] :
		data: List[Tuple[int, int, Optional[str], Optional[str], int, datetime, datetime, int]] = await self.query_async("""
			SELECT
				sets.set_id,
				sets.owner,
				sets.title,
				sets.description,
				sets.privacy,
				sets.created,
				sets.updated,
				set_post.index
			FROM kheina.public.set_post
				INNER JOIN kheina.public.sets
					ON sets.set_id = set_post.set_id
			WHERE set_post.post_id = %s;
			""",
			(post_id.int(),),
			fetch_all=True,
		)

		isets: List[Tuple[InternalSet, int]] = [
			(
				InternalSet(
					set_id=row[0],
					owner=row[1],
					title=row[2],
					description=row[3],
					privacy=await self._id_to_privacy(row[4]),
					created=row[5],
					updated=row[6],
				),
				row[7],  # post index in set
			)
			for row in data
		]

		allowed: List[Tuple[Task[Set], int]] = [
			(ensure_future(iset.set(client, user)), index) for iset, index in isets if await iset.authorized(client, user)
		]

		# TODO: WHAT? first/last/before/after all need to be retrieved here. how the hell do I do that?
		# data: List[Tuple[int, int, Optional[str], Optional[str], int, datetime, datetime]] = await self.query_async("""
		# 	SELECT
		# 		sets.set_id,
		# 		sets.owner,
		# 		sets.title,
		# 		sets.description,
		# 		sets.privacy,
		# 		sets.created,
		# 		sets.updated
		# 	FROM kheina.public.set_post
		# 		INNER JOIN kheina.public.sets
		# 			ON sets.set_id = set_post.set_id
		# 	WHERE set_post.post_id = %s;
		# 	""",
		# 	tuple(iset.set_id for iset, ind in allowed),
		# 	fetch_all=True,
		# )

		sets: List[PostSet] = []

		for set_task, index in allowed :
			set: Set = await set_task
			sets.append(
				PostSet(
					set_id=set.set_id,
					owner=set.owner,
					title=set.title,
					description=set.description,
					privacy=set.privacy,
					created=set.created,
					updated=set.updated,
					post_id=post_id,
					neighbors=PostNeighbors(
						first=None,
						last=None,
						index=index,
						before=[],
						after=[],
					),
				)
			)

		return sets


	async def get_user_sets(self: 'Sets', user: KhUser, handle: str) -> List[Set] :
		owner: int = await client.user_handle_to_id(handle)
		data: List[Tuple[int, int, Optional[str], Optional[str], int, datetime, datetime]] = await self.query_async("""
			SELECT
				sets.set_id,
				sets.owner,
				sets.title,
				sets.description,
				sets.privacy,
				sets.created,
				sets.updated
			FROM kheina.public.sets
			WHERE sets.owner = %s;
			""",
			(owner,),
			fetch_all=True,
		)

		isets: List[InternalSet] = [
			InternalSet(
				set_id=row[0],
				owner=row[1],
				title=row[2],
				description=row[3],
				privacy=await self._id_to_privacy(row[4]),
				created=row[5],
				updated=row[6],
			)
			for row in data
		]

		sets: List[Task[Set]] = [
			ensure_future(iset.set(client, user)) for iset in isets if await iset.authorized(client, user)
		]

		if sets :
			await wait(sets)

		return list(map(Task.result, sets))
