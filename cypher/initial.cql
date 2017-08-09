// unique players
CREATE CONSTRAINT ON (player:Player)
ASSERT player.id IS UNIQUE;

// unique clubs
CREATE CONSTRAINT ON (club:Club)
ASSERT club.id IS UNIQUE;

// unique transfers
CREATE CONSTRAINT ON (t:Transfer)
ASSERT t.id IS UNIQUE;

// load players
LOAD CSV WITH HEADERS FROM "file:///transfers.csv" AS row
MERGE (player:Player {id: row.playerUri})
ON CREATE SET player.name =  row.playerName,
              player.position = row.playerPosition;

// load clubs
LOAD CSV WITH HEADERS FROM "file:///transfers.csv" AS row
WITH row where row.buyerClubUri <> ""
MERGE (club:Club {id: row.buyerClubUri})
ON CREATE SET club.name = row.buyerClubName,
              club.country = row.buyerClubCountry;

LOAD CSV WITH HEADERS FROM "file:///transfers.csv" AS row
WITH row where row.sellerClubUri <> ""
MERGE (club:Club {id: row.sellerClubUri})
ON CREATE SET club.name = row.sellerClubName,
              club.country = row.sellerClubCountry;

// load transfers
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///transfers.csv" AS row
MATCH (player:Player {id: row.playerUri})
MATCH (source:Club {id: row.sellerClubUri})
MATCH (destination:Club {id: row.buyerClubUri})
MERGE (t:Transfer {id: row.transferUri})
ON CREATE SET t.season = row.season, t.rank = row.transferRank,
              t.fee = row.transferFee,  t.timestamp = toInteger(row.timestamp)
MERGE (t)-[ofPlayer:OF_PLAYER]->(player)
SET ofPlayer.age = row.playerAge
MERGE (t)-[:FROM_CLUB]->(source)
MERGE (t)-[:TO_CLUB]->(destination);

// convert numeric transfer fees (with rounding issues)
// MATCH (t:Transfer)
// WITH t, replace(replace(replace(replace(t.fee, "k", ""), "m", ""), "Loan fee:", ""), "£", "") AS rawNumeric
// WITH t,
// CASE
//  WHEN t.fee ENDS WITH "k" THEN toFloat(rawNumeric) * 1000
//  WHEN trim(t.fee) IN ["Free transfer", "ablösefrei ", "gratuito", "free", "free transfer",
//                 "Ablösefrei", "transfervrij", "ablöserei", "Free Transfer", "Libre",
//                 "gratutito", "ablsöefrei", "ablösefrei", "ablösefei", "abösefrei", "Loan",
//                 "draft", "Swap deal", "trade"] THEN 0
//  WHEN NOT(exists(t.fee)) THEN 0
//  ELSE toFloat(rawNumeric) * 1000 * 1000
// END AS numericFee
// SET t.numericFee = numericFee;

// convert numeric transfer fees (with apoc)
MATCH (t:Transfer)
WHERE not(t.fee  contains "?") and not(t.fee  contains "-")
WITH t, replace(replace(replace(replace(t.fee, "k", ""), "m", ""), "Loan fee:", ""), "£", "") AS rawNumeric
WITH t,
CASE
 WHEN t.fee ENDS WITH "k" THEN toFloat(apoc.number.exact.mul(rawNumeric,"1000"))
 WHEN trim(t.fee) IN ["Free transfer", "ablösefrei ", "gratuito", "free", "free transfer",
                      "Ablösefrei", "transfervrij", "ablöserei", "Free Transfer", "Libre",
                      "gratutito", "ablsöefrei", "ablösefrei", "ablösefei", "abösefrei", "Loan",
                      "draft", "Swap deal", "trade"] THEN 0
 WHEN NOT(exists(t.fee)) THEN 0
 ELSE toFloat(apoc.number.exact.mul(rawNumeric,"1000000"))
END AS numericFee
SET t.numericFee = numericFee;

// mark the loans
MATCH (t:Transfer)
WHERE t.fee STARTS WITH 'Loan'
SET t:Loan;

// take these transfers out of the main flow
match (t:Transfer)
WHERE NOT exists(t.numericFee)
remove t:Transfer
SET t:TransferWithoutFee
