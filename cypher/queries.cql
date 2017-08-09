// Transfers by date
match (t:Transfer)
return apoc.date.format(t.timestamp,'s', 'yyyy-MM-dd', "GMT+01:00"), count(*) AS count
ORDER BY count DESC;
