WITH new_users AS (
    INSERT INTO users (f3_name, email)
    VALUES
      ('Alice', 'alice@gmail.com'),
      ('Bob',   'bob@gmail.com')
    RETURNING id, email, f3_name
)
INSERT INTO slack_users (user_id, slack_id, user_name, email, is_admin, is_owner, is_bot, slack_team_id)
SELECT
    u.id,
    s.slack_id,
    u.f3_name,
    u.email,
    FALSE,
    FALSE,
    FALSE,
    s.slack_team_id
FROM new_users u
JOIN (
    VALUES
      ('alice@gmail.com', 'U123456', 'T09198JKA2J'),
      ('bob@gmail.com',   'U789012', 'T09198JKA2J')
) AS s(email, slack_id, slack_team_id)
  ON s.email = u.email;
