call apoc.load.json("https://github.com/mneedham/football-transfers/raw/master/data/transfers.json")
YIELD value
WITH value, apoc.text.replace(value.transfer.value, "£", "") AS transferValue
WHERE transferValue <> "?" AND transferValue <> "-"

MERGE (p:Player {id: value.player.href})
SET p.name = value.player.name, p.image = value.player.image

MERGE (from:Club {id: value.from.href})
SET from.name = value.from.name

FOREACH(ignoreMe IN CASE WHEN value.from.leagueHref = "" THEN [] ELSE [1] END |
  MERGE (fromLeague:League {id: value.from.leagueId})
  SET fromLeague.name = value.from.league
  MERGE (from)-[:IN_LEAGUE]->(fromLeague)
)

FOREACH(ignoreMe IN CASE WHEN value.from.countryImage = "" THEN [] ELSE [1] END |
  MERGE (fromCountry:Country {name: value.from.country})
  SET fromCountry.image = value.from.countryImage
)

MERGE (to:Club {id: value.to.href})
SET to.name = value.to.name

FOREACH(ignoreMe IN CASE WHEN value.to.leagueHref = "" THEN [] ELSE [1] END |
  MERGE (toLeague:League {id: value.to.leagueId})
  SET toLeague.name = value.to.league
  MERGE (to)-[:IN_LEAGUE]->(toLeague)
)

FOREACH(ignoreMe IN CASE WHEN value.to.countryImage = "" THEN [] ELSE [1] END |
  MERGE (toCountry:Country {name: value.to.country})
  SET toCountry.image = value.to.countryImage
)

MERGE (t:Transfer {id: value.transfer.href})
SET t.value = CASE
       WHEN transferValue contains "k" THEN toFloat(apoc.text.replace(transferValue, "k", "")) * 1000
       WHEN transferValue contains "m" THEN toFloat(apoc.text.replace(transferValue, "m", "")) * 1000000
       ELSE 0.0 END
SET t.date = date(datetime({epochseconds:value.transfer.timestamp}))

MERGE (t)-[:OF_PLAYER]->(p)
MERGE (t)-[:FROM_CLUB]->(from)
MERGE (t)-[:TO_CLUB]->(to);

call apoc.load.json("https://github.com/mneedham/football-transfers/raw/master/data/leagues.json")
YIELD value
MATCH (l:League {id: value.league.id})
SET l.image = value.image, l.name = value.league.name
MERGE (c:Country {name: value.country})
MERGE (l)-[:IN_COUNTRY]->(c);