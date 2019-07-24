// Transfers by date
match (t:Transfer)
return apoc.date.format(t.timestamp,'s', 'yyyy-MM-dd', "GMT+01:00"), count(*) AS count
ORDER BY count DESC;


MATCH (t:Transfer)-[:OF_PLAYER]->(player), (from)<-[:FROM_CLUB]-(t)-[:TO_CLUB]->(to)
RETURN player.name, apoc.number.format(t.value) AS fee, from.name, to.name
ORDER By t.value DESC
LIMIT 25;


// Selling club
MATCH (t:Transfer)-[:OF_PLAYER]->(player), (from)<-[:FROM_CLUB]-(t)-[:TO_CLUB]->(to)
WITH from, sum(t.value) AS totalValue
RETURN from.name, apoc.number.format(totalValue)
ORDER By totalValue DESC
LIMIT 100;

// money in/money out
MATCH (club:Club)
WITH club,
     apoc.coll.sumLongs([(club)<-[:FROM_CLUB]-(t) | t.value]) AS moneyIn,
     apoc.coll.sumLongs([(club)<-[:TO_CLUB]-(t) | t.value]) AS moneyOut
RETURN club.name, apoc.number.format(moneyIn) AS in, apoc.number.format(moneyOut) AS out
ORDER BY moneyIn + moneyOut DESC