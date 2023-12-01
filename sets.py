from asyncio import Task, ensure_future, wait
from collections import defaultdict
from enum import Enum
from typing import Dict, List, Optional, Tuple, Union

from fuzzly.internal import InternalClient
from fuzzly.models.internal import InternalPost, InternalPosts, InternalSet, SetKVS
from fuzzly.models.post import MediaType, Post, PostId, PostSize, Privacy, Rating
from fuzzly.models.set import PostSet, Set, SetId, SetNeighbors
from fuzzly.models.user import UserPrivacy
from kh_common.auth import KhUser, Scope
from kh_common.caching import AerospikeCache, ArgsCache
from kh_common.config.credentials import fuzzly_client_token
from kh_common.datetime import datetime
from kh_common.exceptions.http_error import BadRequest, HttpErrorHandler, NotFound
from kh_common.hashing import Hashable
from kh_common.sql import SqlInterface

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
	UNIQUE (set_id, index) INCLUDE (post_id)
);
"""

set_not_found_err: str = 'no data was found for the provided set id: {set_id}.'

client: InternalClient = InternalClient(fuzzly_client_token)


class Sets(SqlInterface, Hashable) :

	def __init__(self: 'Sets') -> None :
		SqlInterface.__init__(
			self,
			conversions={
				Enum: lambda x: x.name,
				PostId: int,
				SetId: int,
			},
		)
		Hashable.__init__(self)


	async def _verify_authorized(user: KhUser, iset: InternalSet) -> bool :
		return user.user_id == iset.set_id or await user.verify_scope(Scope.mod, raise_error=False)


	@ArgsCache(float('inf'))
	async def _id_to_privacy(self: 'Sets', privacy_id: int) -> Privacy :
		data: Tuple[str] = await self.query_async("""
			SELECT
				type
			FROM kheina.public.privacy
			WHERE privacy.privacy_id = %s;
			""",
			(privacy_id,),
			fetch_one=True,
		)

		return Privacy(data[0])


	@ArgsCache(float('inf'))
	async def _id_to_rating(self: 'Sets', rating_id: int) -> Rating :
		data: Tuple[str] = await self.query_async("""
			SELECT
				rating
			FROM kheina.public.ratings
			WHERE ratings.rating_id = %s;
			""",
			(rating_id,),
			fetch_one=True,
		)

		return Rating(data[0])


	@ArgsCache(float('inf'))
	async def _id_to_media_type(self: 'Sets', media_type_id: int) -> MediaType :
		if media_type_id is None :
			return None

		data: Tuple[str, str] = await self.query_async("""
			SELECT
				file_type,
				mime_type
			FROM kheina.public.media_type
			WHERE media_type.media_type_id = %s;
			""",
			(media_type_id,),
			fetch_one=True,
		)

		return MediaType(
			file_type = data[0],
			mime_type = data[1],
		)


	@ArgsCache(float('inf'))
	async def _id_to_set_privacy(self: 'Sets', privacy_id: int) -> UserPrivacy :
		data: Tuple[str] = await self.query_async("""
			SELECT
				type
			FROM kheina.public.privacy
			WHERE privacy.privacy_id = %s;
			""",
			(privacy_id,),
			fetch_one=True,
		)

		return UserPrivacy(data[0])


	@HttpErrorHandler('creating a set')
	async def create_set(self: 'Sets', user: KhUser, title: str, privacy: UserPrivacy, description: Optional[str]) -> Set :
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
			count=0,
			title=title,
			description=description,
			privacy=privacy,
			created=data[0],
			updated=data[1],
		)

		ensure_future(SetKVS.put_async(set_id, iset))
		return await iset.set(client, user)


	@AerospikeCache('kheina', 'sets', '{set_id}', _kvs=SetKVS)
	async def _get_set(self: 'Sets', set_id: SetId) -> InternalSet :
		data: Tuple[int, Optional[str], Optional[str], int, datetime, datetime] = await self.query_async("""
			WITH f AS (
				SELECT post_id AS first, index
				FROM kheina.public.set_post
				WHERE set_id = %s
				ORDER BY set_post.index ASC
				LIMIT 1
			), l AS (
				SELECT post_id AS last, index
				FROM kheina.public.set_post
				WHERE set_id = %s
				ORDER BY set_post.index DESC
				LIMIT 1
			)
			SELECT
				owner,
				title,
				description,
				privacy,
				created,
				updated,
				f.first,
				l.last,
				l.index
			FROM kheina.public.sets
				JOIN f
				JOIN l
			WHERE sets.set_id = %s;
			""",
			(set_id.int(), set_id.int()),
			fetch_one=True,
		)

		if not data: 
			raise NotFound(set_not_found_err.format(set_id=set_id))

		return InternalSet(
			set_id=set_id,
			owner=data[0],
			title=data[1],
			description=data[2],
			privacy=await self._id_to_set_privacy(data[3]),
			created=data[4],
			updated=data[5],
			first=data[6],
			last=data[7],
			count=data[8] + 1,  # set indices are 0-indexed, so add one
		)


	@HttpErrorHandler('retrieving set')
	async def get_set(self: 'Sets', user: KhUser, set_id: SetId) -> Set :
		iset: InternalSet = await self._get_set(set_id)

		if await iset.authorized(client, user) :
			return await iset.set(client, user)

		raise NotFound(set_not_found_err.format(set_id=set_id))


	@HttpErrorHandler('updating a set')
	async def update_set(self: 'Sets', user: KhUser, set_id: SetId, req: UpdateSetRequest) :
		iset: InternalSet = await self._get_set(set_id)

		if not Sets._verify_authorized(user, iset) :
			raise NotFound(set_not_found_err.format(set_id=set_id))

		params: List[Union[str, UserPrivacy, None]] = []
		bad_mask: List[str] = []
		query: List[str] = []

		for m in req.mask :

			if m == 'owner' :
				owner: int = await client.user_handle_to_id(req.owner)
				params.append(owner)
				iset.owner = owner
				query.append(m + ' = %s')

			elif m == 'title' :
				params.append(req.title)
				iset.title = req.title
				query.append(m + ' = %s')

			elif m == 'description' :
				params.append(req.description)
				iset.description = req.description
				query.append(m + ' = %s')

			elif m == 'privacy' :
				params.append(req.privacy)
				iset.privacy = req.privacy
				query.append(m + ' = privacy_to_id(%s)')

			else :
				bad_mask.append(m)

		if bad_mask :
			if len(bad_mask) == 1 :
				raise BadRequest(f'[{bad_mask[0]}] is not a valid mask value')

			else :
				raise BadRequest(f'[{", ".join(bad_mask)}] are not valid mask values')

		params.append(set_id.int())
		query.append('updated = now()')

		data: Tuple[datetime] = await self.query_async(f"""
			UPDATE kheina.public.sets
				SET {', '.join(query)}
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
			raise NotFound(set_not_found_err.format(set_id=set_id))

		await self.query_async("""
			DELETE FROM kheina.public.set_post
			WHERE set_id = %s;
			DELETE FROM kheina.public.sets
			WHERE set_id = %s;
			""",
			(set_id.int(), set_id.int()),
			commit=True,
		)

		await SetKVS.remove_async(set_id)


	async def add_post_to_set(self: 'Sets', user: KhUser, post_id: PostId, set_id: SetId, index: int) -> None :
		iset_task: Task[InternalSet] = ensure_future(self._get_set(set_id))
		ipost: InternalPost = await client.post(post_id)

		if not await ipost.authorized(client, user) :
			raise NotFound(f'no data was found for the provided post id: {post_id}.')

		iset: InternalSet = await iset_task

		if not await iset.authorized(client, user) :
			raise NotFound(set_not_found_err.format(set_id=set_id))

		await self.query_async("""
			WITH i AS (
				SELECT
					least(%s, count(1)) AS index
				FROM kheina.public.set_post
					WHERE set_id = %s
			), _ AS (
				UPDATE kheina.public.set_post
					SET index = set_post.index + 1
				FROM i
				WHERE set_post.set_id = %s
					AND set_post.index >= i.index
			)
			INSERT INTO kheina.public.set_post
			(set_id, post_id, index)
			SELECT
				%s, %s, i.index
			FROM i;
			""",
			(
				index, set_id.int(),
				set_id.int(),
				set_id.int(), post_id.int(),
			),
			commit=True,
		)

		iset.count += 1
		ensure_future(SetKVS.put_async(set_id, iset))


	async def remove_post_from_set(self: 'Sets', user: KhUser, post_id: PostId, set_id: SetId) -> None :
		iset_task: Task[InternalSet] = ensure_future(self._get_set(set_id))
		ipost: InternalPost = await client.post(post_id)

		if not await ipost.authorized(client, user) :
			raise NotFound(f'no data was found for the provided post id: {post_id}.')

		iset: InternalSet = await iset_task

		if not await iset.authorized(client, user) :
			raise NotFound(set_not_found_err.format(set_id=set_id))

		await self.query_async("""
			WITH deleted AS (
				DELETE FROM kheina.public.set_post
				WHERE set_id = %s
					AND post_id = %s
				RETURNING index
			)
			UPDATE kheina.public.set_post
				SET index = set_post.index - 1
			FROM deleted
			WHERE set_post.set_id = %s
				AND set_post.index >= deleted.index;
			""",
			(
				set_id.int(), post_id.int(),
				set_id.int(),
			),
			commit=True,
		)

		iset.count -= 1
		ensure_future(SetKVS.put_async(set_id, iset))


	async def get_post_sets(self: 'Sets', user: KhUser, post_id: PostId) -> List[PostSet] :
		neighbor_range: int = 3  # const
		data: List[Tuple[
			int, int, Optional[str], Optional[str], int, datetime, datetime,  # set
			int, int,  # post index
			int, Optional[str], Optional[str], int, int, datetime, datetime, Optional[str], int, int, int, int, int,  # posts
		]] = await self.query_async("""
			WITH post_sets AS (
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
				WHERE set_post.post_id = %s
			), f AS (
				SELECT set_post.set_id, set_post.post_id AS first, set_post.index
				FROM post_sets
					INNER JOIN kheina.public.set_post
						ON set_post.set_id = post_sets.set_id
				ORDER BY set_post.index ASC
				LIMIT 1
			), l AS (
				SELECT set_post.set_id, set_post.post_id AS last, set_post.index
				FROM post_sets
					INNER JOIN kheina.public.set_post
						ON set_post.set_id = post_sets.set_id
				ORDER BY set_post.index DESC
				LIMIT 1
			)
			SELECT
				post_sets.set_id,
				post_sets.owner,
				post_sets.title,
				post_sets.description,
				post_sets.privacy,
				post_sets.created,
				post_sets.updated,
				post_sets.index,
				set_post.index,
				posts.post_id,
				posts.title,
				posts.description,
				posts.rating,
				posts.parent,
				posts.created_on,
				posts.updated_on,
				posts.filename,
				posts.media_type_id,
				posts.width,
				posts.height,
				posts.uploader,
				posts.privacy_id,
				f.first,
				l.last,
				l.index
			FROM post_sets
				INNER JOIN kheina.public.set_post
					ON set_post.set_id = post_sets.set_id
					AND set_post.index BETWEEN post_sets.index - %s AND post_sets.index + %s
					AND set_post.index != post_sets.index
				INNER JOIN kheina.public.posts
					ON posts.post_id = set_post.post_id
				INNER JOIN first
					ON posts.post_id = set_post.post_id
				INNER JOIN f
					ON f.set_id = post_sets.set_id
				INNER JOIN l
					ON l.set_id = post_sets.set_id
			""",
			(
				post_id.int(),
				neighbor_range, neighbor_range,
			),
			fetch_all=True,
		)

		# both tuples are formatted: index, object. set is the index of the parent post. posts is index of the neighbors
		isets: List[Tuple[int, InternalSet]] = []
		posts: Dict[int, List[Tuple[int, InternalPost]]] = defaultdict(lambda : [])

		sets_made: set = set()
		for row in data :
			if row[0] not in sets_made :
				sets_made.add(row[0])
				isets.append((
					row[7],
					InternalSet(
						set_id=row[0],
						owner=row[1],
						title=row[2],
						description=row[3],
						privacy=await self._id_to_set_privacy(row[4]),
						created=row[5],
						updated=row[6],
						first=row[22],
						last=row[23],
						count=row[24] + 1,
					),
				))

			posts[row[0]].append((
				row[8],
				InternalPost(
					post_id=row[9],
					title=row[10],
					description=row[11],
					rating=await self._id_to_rating(row[12]),
					parent=row[13],
					created=row[14],
					updated=row[15],
					filename=row[16],
					media_type=await self._id_to_media_type(row[17]),
					size=PostSize(
						width=row[18],
						height=row[19],
					) if row[18] and row[19] else None,
					user_id=row[20],
					privacy=await self._id_to_privacy(row[21]),
				),
			))

		# again, this is index, set task
		allowed: List[Tuple[int, Task[Set]]] = [
			(index, ensure_future(iset.set(client, user))) for index, iset in isets if await iset.authorized(client, user)
		]

		sets: List[PostSet] = []

		for index, set_task in allowed :
			s: Set = await set_task
			before: Task[List[Post]] = ensure_future(InternalPosts(post_list=list(map(lambda x : x[1], sorted(filter(lambda x : x[0] < index, posts[s.set_id.int()]), key=lambda x : x[0], reverse=True)))).posts(client, user))
			after: Task[List[Post]] = ensure_future(InternalPosts(post_list=list(map(lambda x : x[1], sorted(filter(lambda x : x[0] > index, posts[s.set_id.int()]), key=lambda x : x[0], reverse=False)))).posts(client, user))
			sets.append(
				PostSet(
					set_id=s.set_id,
					owner=s.owner,
					title=s.title,
					description=s.description,
					privacy=s.privacy,
					created=s.created,
					updated=s.updated,
					count=s.count,
					first=s.first,
					last=s.last,
					neighbors=SetNeighbors(
						index=index,
						before=await before,
						after=await after,
					),
				)
			)

		return sets


	async def get_user_sets(self: 'Sets', user: KhUser, handle: str) -> List[Set] :
		owner: int = await client.user_handle_to_id(handle)
		# TODO: MISSING FINAL SELECT
		data: List[Tuple[int, int, Optional[str], Optional[str], int, datetime, datetime]] = await self.query_async("""
			WITH user_sets AS (
				SELECT
					sets.set_id,
					sets.owner,
					sets.title,
					sets.description,
					sets.privacy,
					sets.created,
					sets.updated
				FROM kheina.public.sets
				WHERE sets.owner = %s
			), f AS (
				SELECT set_post.set_id, post_id AS first, index
				FROM user_sets
					INNER JOIN kheina.public.set_post
						ON set_post.set_id = user_sets.set_id
				ORDER BY set_post.index ASC
				LIMIT 1
			), l AS (
				SELECT set_post.set_id, post_id AS last, index
				FROM user_sets
					INNER JOIN kheina.public.set_post
						ON set_post.set_id = user_sets.set_id
				ORDER BY set_post.index DESC
				LIMIT 1
			)
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
				privacy=await self._id_to_set_privacy(row[4]),
				created=row[5],
				updated=row[6],
				count=0 if row[7] is None else row[7] + 1,  # set indices are 0-indexed, so add one
			)
			for row in data
		]

		sets: List[Task[Set]] = [
			ensure_future(iset.set(client, user)) for iset in isets if await iset.authorized(client, user)
		]

		if sets :
			await wait(sets)

		return list(map(Task.result, sets))
