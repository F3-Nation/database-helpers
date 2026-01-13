select
	u.email
from
	users u
left join slack_users su on
	su.user_id = u.id
where
	(f3_name = ''
		or f3_name is null )
	and su.slack_id is not null
	and (u.email is not null
		or u.email != '')