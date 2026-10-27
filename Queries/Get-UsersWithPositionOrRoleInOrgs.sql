-- Users holding a position or a role in any of the given orgs.
-- Edit the org id list in target_orgs, then run the whole statement in DBeaver.
-- One row per user / org / grant. See the bottom for a "distinct users only" variant.

with target_orgs (org_id) as (
	values
		(101),   -- <-- replace with your org ids
		(102),
		(103)
),
grants as (
	-- positions held in one of the target orgs
	select
		pou.user_id,
		pou.org_id,
		'position'::text as grant_type,
		p.name::text as grant_name,
		p.is_active as position_is_active
	from positions_x_orgs_x_users pou
	join positions p on p.id = pou.position_id
	where pou.org_id in (select org_id from target_orgs)

	union all

	-- roles held in one of the target orgs
	select
		ruo.user_id,
		ruo.org_id,
		'role'::text as grant_type,
		r.name::text as grant_name,
		null::boolean as position_is_active
	from roles_x_users_x_org ruo
	join roles r on r.id = ruo.role_id
	where ruo.org_id in (select org_id from target_orgs)
)
select
	u.id as user_id,
	u.f3_name,
	u.first_name,
	u.last_name,
	u.email,
	u.status,
	g.org_id,
	o.name as org_name,
	o.org_type,
	g.grant_type,
	g.grant_name,
	g.position_is_active
from grants g
join users u on u.id = g.user_id
join orgs o on o.id = g.org_id
order by
	o.name,
	u.last_name,
	u.first_name,
	g.grant_type,
	g.grant_name;
