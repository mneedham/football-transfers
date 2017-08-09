// unique players
CREATE CONSTRAINT ON (player:Player)
ASSERT player.id IS UNIQUE;

// unique clubs
CREATE CONSTRAINT ON (club:Club)
ASSERT club.id IS UNIQUE;

// unique transfers
CREATE CONSTRAINT ON (t:Transfer)
ASSERT t.id IS UNIQUE;
