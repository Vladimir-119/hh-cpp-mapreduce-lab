-- Independent variant 6 condition. No Python UDF or prepared variant flag.
-- RE2 has no lookaround: use explicit Unicode letter/digit boundaries.
CREATE TEMP TABLE selected AS
WITH cleaned AS (
    SELECT *, regexp_replace(
      regexp_replace(
        regexp_replace(name,
          '(^|[^\p{L}\p{N}_])1[\s-]*[cс]($|[^\p{L}\p{N}_])', '\1 \2', 'gi'),
        'objective[\s-]*[cс]', '', 'gi'),
      '[CС]\s*#', '', 'g') AS clean_name
    FROM raw
)
SELECT * EXCLUDE (key_skills, clean_name),
       list_sort(list_distinct(list_filter(
         list_transform(key_skills, x -> lower(trim(x))), x -> x <> ''
       ))) AS skills
FROM cleaned
WHERE professional_roles_id = '96' AND (
    regexp_matches(name, '(^|[^\p{L}\p{N}_])[cсCС]\+\+($|[^+])')
 OR regexp_matches(clean_name, '(^|[^\p{L}\p{N}_.])[CС]($|[^\p{L}\p{N}_.#+])')
 OR regexp_matches(clean_name, '(^|[^\p{L}\p{N}_])си($|[^\p{L}\p{N}_])', 'i')
 OR regexp_matches(clean_name, '(^|[^\p{L}\p{N}_])c\s+(developer|programmer)($|[^\p{L}\p{N}_])')
);

