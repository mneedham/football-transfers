// add nationality
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///transfers.csv" AS row
MATCH (player:Player {id: row.playerUri})
SET player.nationality = row.playerNationality;
