-- One row per skill position. The list is sorted and deduplicated in variant.sql.
CREATE TEMP TABLE units AS
SELECT id, i, skills[i] AS skill
FROM selected, range(1, len(skills) + 1) AS positions(i);

CREATE TEMP TABLE counts1 AS
SELECT [skill] AS skills, count(*)::BIGINT AS n
FROM units GROUP BY skills;

CREATE TEMP TABLE counts2 AS
SELECT [a.skill, b.skill] AS skills, count(*)::BIGINT AS n
FROM units a JOIN units b ON a.id=b.id AND a.i<b.i
GROUP BY skills;

CREATE TEMP TABLE counts3 AS
SELECT [a.skill, b.skill, c.skill] AS skills, count(*)::BIGINT AS n
FROM units a
JOIN units b ON a.id=b.id AND a.i<b.i
JOIN units c ON a.id=c.id AND b.i<c.i
GROUP BY skills;

